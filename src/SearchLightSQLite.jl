module SearchLightSQLite

import SQLite, DataFrames, Logging
import SearchLight
import DBInterface


#
# Setup
#


const COLUMN_NAME_FIELD_NAME = :name

function SearchLight.column_field_name()
  COLUMN_NAME_FIELD_NAME
end

const DatabaseHandle = SQLite.DB
const ResultHandle   = Union{Vector{Any}, DataFrames.DataFrame, Vector{Tuple}, Vector{Tuple{Int64}}}

const TYPE_MAPPINGS = Dict{Symbol,Symbol}( # Julia => SQLite
  :char       => :CHARACTER,
  :string     => :VARCHAR,
  :text       => :TEXT,
  :integer    => :INTEGER,
  :int        => :INTEGER,
  :float      => :FLOAT,
  :decimal    => :DECIMAL,
  :datetime   => :DATETIME,
  :timestamp  => :INTEGER,
  :time       => :TIME,
  :date       => :DATE,
  :binary     => :BLOB,
  :boolean    => :BOOLEAN,
  :bool       => :BOOLEAN
)

const SELECT_LAST_ID_QUERY_START = "; SELECT CASE WHEN last_insert_rowid() = 0 THEN"
const SELECT_LAST_ID_QUERY_END = "ELSE last_insert_rowid() END AS $(SearchLight.LAST_INSERT_ID_LABEL)"

const CONNECTIONS = DatabaseHandle[]


#
# Connection
#


"""
    connect(conn_data::Dict)::DatabaseHandle
    function connect()::DatabaseHandle

Connects to the database defined in conn_data["filename"] and returns a handle.
If no conn_data is provided, a temporary, in-memory database will be used.
"""
function SearchLight.connect(conn_data::Dict = SearchLight.config.db_config_settings) :: DatabaseHandle
  if ! haskey(conn_data, "filename")
    conn_data["filename"] = if haskey(conn_data, "host") && conn_data["host"] != nothing
                              conn_data["host"]
                            elseif haskey(conn_data, "database") && conn_data["database"] != nothing
                              conn_data["database"]
                            end
  end

  push!(CONNECTIONS, SQLite.DB(conn_data["filename"]))[end]
end


"""
    disconnect(conn::DatabaseHandle)::Nothing

Disconnects from database.
"""
function SearchLight.disconnect(conn::DatabaseHandle = SearchLight.connection()) :: Nothing
  conn = nothing
end


function SearchLight.connection()
  isempty(CONNECTIONS) && throw(SearchLight.Exceptions.NotConnectedException())

  CONNECTIONS[end]
end


#
# Utility
#


"""
    columns{T<:AbstractModel}(m::Type{T})::DataFrames.DataFrame
    columns{T<:AbstractModel}(m::T)::DataFrames.DataFrame

Returns a DataFrame representing schema information for the database table columns associated with `m`.
"""
function SearchLight.columns(m::Type{T})::DataFrames.DataFrame where {T<:SearchLight.AbstractModel}
  SearchLight.query(table_columns_sql(SearchLight.table(m)), internal = true)
end


"""
    table_columns_sql(table_name::String)::String

Returns the adapter specific query for SELECTing table columns information corresponding to `table_name`.
"""
function table_columns_sql(table_name::String) :: String
  "PRAGMA table_info(`$table_name`)"
end


#
# Data sanitization
#


"""
    escape_column_name(c::String, conn::DatabaseHandle)::String

Escapes the column name using native features provided by the database backend.

# Examples
```julia
julia>
```
"""
function SearchLight.escape_column_name(c::String, conn::DatabaseHandle = SearchLight.connection()) :: String
  join([SQLite.esc_id(c) for c in split(c, '.')], '.')
end


function SearchLight.escape_value(v::T, conn::DatabaseHandle = SearchLight.connection())::T where {T}
  isa(v, Number) ? v : "'$(replace(string(v), "'"=>"''"))'"
end


#
# Query execution
#


"""
    query(sql::String, suppress_output::Bool, conn::DatabaseHandle)::DataFrames.DataFrame

Executes the `sql` query against the database backend and returns a DataFrame result.

# Examples:
```julia
julia> query(SearchLight.to_fetch_sql(Article, SQLQuery(limit = 5)), false, Database.connection)

2017-01-16T21:36:21.566 - info: SQL QUERY: SELECT \"articles\".\"id\" AS \"articles_id\", \"articles\".\"title\" AS \"articles_title\", \"articles\".\"summary\" AS \"articles_summary\", \"articles\".\"content\" AS \"articles_content\", \"articles\".\"updated_at\" AS \"articles_updated_at\", \"articles\".\"published_at\" AS \"articles_published_at\", \"articles\".\"slug\" AS \"articles_slug\" FROM \"articles\" LIMIT 5

  0.000985 seconds (16 allocations: 576 bytes)

5×7 DataFrames.DataFrame
...
```
"""
function SearchLight.query(sql::String, conn::DatabaseHandle = SearchLight.connection(); internal = false) :: DataFrames.DataFrame
  parts::Vector{String} = if occursin(SELECT_LAST_ID_QUERY_START, sql)
                            split(sql, SELECT_LAST_ID_QUERY_START)
                          else
                            String[sql]
                          end

  length(parts) == 2 && (parts[2] = string(SELECT_LAST_ID_QUERY_START, parts[2]))

  if SearchLight.config.log_queries && ! internal
    if length(parts) == 2
      @info parts[1]
      @time DBInterface.execute(conn, parts[1]) |> DataFrames.DataFrame

      @info parts[2]
      @time DBInterface.execute(conn, parts[2]) |> DataFrames.DataFrame
    else
      @info parts[1]
      @time DBInterface.execute(conn, parts[1]) |> DataFrames.DataFrame
    end
  else
    if length(parts) == 2
      DBInterface.execute(conn, parts[1]) |> DataFrames.DataFrame
      DBInterface.execute(conn, parts[2]) |> DataFrames.DataFrame
    else
      DBInterface.execute(conn, parts[1]) |> DataFrames.DataFrame
    end
  end
end


function SearchLight.to_find_sql(m::Type{T}, q::SearchLight.SQLQuery, joins::Union{Nothing,Vector{SearchLight.SQLJoin{N}}} = nothing)::String where {T<:SearchLight.AbstractModel, N<:Union{Nothing,SearchLight.AbstractModel}}
  sql::String = ( string("$(SearchLight.to_select_part(m, q.columns, joins)) $(SearchLight.to_from_part(m)) $(SearchLight.to_join_part(m, joins)) $(SearchLight.to_where_part(q.where)) ",
                      "$(SearchLight.to_group_part(q.group)) $(SearchLight.to_having_part(q.having)) $(SearchLight.to_order_part(m, q.order)) ",
                      "$(SearchLight.to_limit_part(q.limit)) $(SearchLight.to_offset_part(q.offset))")) |> strip
  replace(sql, r"\s+"=>" ")
end


function SearchLight.to_store_sql(m::T; conflict_strategy = :error)::String where {T<:SearchLight.AbstractModel}
  uf = SearchLight.persistable_fields(typeof(m))

  sql = if ! SearchLight.ispersisted(m) || (SearchLight.ispersisted(m) && conflict_strategy == :update)
    pos = findfirst(x -> x == SearchLight.primary_key_name(typeof(m)), uf)
    pos > 0 && splice!(uf, pos)

    fields = SearchLight.SQLColumn(uf)
    vals = join( map(x -> string(SearchLight.to_sqlinput(m, Symbol(x), getfield(m, Symbol(x)))), uf), ", ")

    "INSERT $(conflict_strategy == :ignore ? " OR IGNORE" : "") INTO $(SearchLight.table(typeof(m))) ($fields) VALUES ($vals)"
  else
    "UPDATE $(SearchLight.table(typeof(m))) SET $(SearchLight.update_query_part(m))"
  end

  string( sql, raw" ",
          SELECT_LAST_ID_QUERY_START, raw" ",
          getfield(m, Symbol(SearchLight.primary_key_name(m))).value === nothing ? -1 : getfield(m, Symbol(SearchLight.primary_key_name(m))).value, raw" ",
          SELECT_LAST_ID_QUERY_END)
end


function SearchLight.delete_all(m::Type{T}; truncate::Bool = true, reset_sequence::Bool = true, cascade::Bool = false)::Nothing where {T<:SearchLight.AbstractModel}
  "DELETE FROM $(SearchLight.table(m))" |> SearchLight.query

  nothing
end


function SearchLight.delete(m::T)::T where {T<:SearchLight.AbstractModel}
  SearchLight.ispersisted(m) || throw(SearchLight.Exceptions.NotPersistedException(m))

  "DELETE FROM $(SearchLight.table(typeof(m))) WHERE $(SearchLight.primary_key_name(typeof(m))) = '$(m.id.value)'" |> SearchLight.query

  m.id = SearchLight.DbId()

  m
end


function Base.count(m::Type{T}, q::SearchLight.SQLQuery = SearchLight.SQLQuery())::Int where {T<:SearchLight.AbstractModel}
  count_column = SearchLight.SQLColumn("COUNT(*) AS __cid", raw = true)
  q = SearchLight.clone(q, :columns, push!(q.columns, count_column))

  SearchLight.DataFrame(m, q)[1, Symbol("__cid")]
end


function SearchLight.update_query_part(m::T)::String where {T<:SearchLight.AbstractModel}
  update_values = join(map(x -> "$(string(SearchLight.SQLColumn(x))) = $(string(SearchLight.to_sqlinput(m, Symbol(x), getfield(m, Symbol(x)))) )", SearchLight.persistable_fields(typeof(m))), ", ")

  " $update_values WHERE $(SearchLight.table(typeof(m))).$(SearchLight.primary_key_name(typeof(m))) = '$(m.id.value)'"
end


function SearchLight.column_data_to_column_name(column::SearchLight.SQLColumn, column_data::Dict{Symbol,Any}) :: String
  "$(SearchLight.to_fully_qualified(column_data[:column_name], column_data[:table_name])) AS $(isempty(column_data[:alias]) ? SearchLight.to_sql_column_name(column_data[:column_name], column_data[:table_name]) : column_data[:alias] )"
end


function SearchLight.to_from_part(m::Type{T})::String where {T<:SearchLight.AbstractModel}
  string("FROM ", SearchLight.escape_column_name(SearchLight.table(m), SearchLight.connection()))
end


function SearchLight.to_where_part(w::Vector{SearchLight.SQLWhereEntity}) :: String
  where = isempty(w) ?
          "" :
          string("WHERE ",
                  (string(first(w).condition) == "AND" ? "TRUE " : "FALSE "),
                  join(map(wx -> string(wx), w), " "))

  replace(where, r"WHERE TRUE AND "i => "WHERE ")
end


function SearchLight.to_order_part(m::Type{T}, o::Vector{SearchLight.SQLOrder})::String where {T<:SearchLight.AbstractModel}
  isempty(o) ?
    "" :
    string("ORDER BY ",
            join(map(x -> string((! SearchLight.is_fully_qualified(x.column.value) ?
                                    SearchLight.to_fully_qualified(m, x.column) :
                                    x.column.value), " ", x.direction),
                      o), ", "))
end


function SearchLight.to_group_part(g::Vector{SearchLight.SQLColumn}) :: String
  isempty(g) ?
    "" :
    string(" GROUP BY ", join(map(x -> string(x), g), ", "))
end


function SearchLight.to_limit_part(l::SearchLight.SQLLimit) :: String
  l.value != "ALL" ? string("LIMIT ", string(l)) : ""
end


function SearchLight.to_offset_part(o::Int) :: String
  o != 0 ? string("OFFSET ", string(o)) : ""
end


function SearchLight.to_having_part(h::Vector{SearchLight.SQLWhereEntity}) :: String
  having =  isempty(h) ?
            "" :
            string("HAVING ",
                    (string(first(h).condition) == "AND" ? "TRUE " : "FALSE "),
                    join(map(w -> string(w), h), " "))

  replace(having, r"HAVING TRUE AND "i => "HAVING ")
end


function SearchLight.to_join_part(m::Type{T}, joins::Union{Nothing,Vector{SearchLight.SQLJoin{N}}} = nothing)::String where {T<:SearchLight.AbstractModel, N<:Union{Nothing,SearchLight.AbstractModel}}
  joins === nothing && return ""

  join(map(x -> string(x), joins), " ")
end


function Base.rand(m::Type{T}; limit = 1)::Vector{T} where {T<:SearchLight.AbstractModel}
  SearchLight.find(m, SearchLight.SQLQuery(limit = SearchLight.SQLLimit(limit), order = [SearchLight.SQLOrder("random()", raw = true)]))
end


#### MIGRATIONS ####


"""
    create_migrations_table(table_name::String)::Nothing

Runs a SQL DB query that creates the table `table_name` with the structure needed to be used as the DB migrations table.
The table should contain one column, `version`, unique, as a string of maximum 30 chars long.
"""
function SearchLight.Migration.create_migrations_table(table_name::String = SearchLight.config.db_migrations_table_name) :: Nothing
  SearchLight.query(
    "CREATE TABLE `$table_name` (
      `version` varchar(30) NOT NULL DEFAULT '',
      PRIMARY KEY (`version`)
    )", internal = true)

  @info "Created table $table_name"

  nothing
end


function SearchLight.Migration.create_table(f::Function, name::Union{String,Symbol}, options::Union{String,Symbol} = "") :: Nothing
  SearchLight.query(create_table_sql(f, string(name), options), internal = true)

  nothing
end


function create_table_sql(f::Function, name::Union{String,Symbol}, options::Union{String,Symbol} = "") :: String
  "CREATE TABLE $name (" * join(f()::Vector{String}, ", ") * ") $options" |> strip
end


function SearchLight.Migration.column(name::Union{String,Symbol}, column_type::Union{String,Symbol}, options::Any = ""; default::Any = nothing, limit::Union{Int,Nothing,String} = nothing, not_null::Bool = false) :: String
  "$name $(TYPE_MAPPINGS[column_type] |> string) " *
    (isa(limit, Int) ? "($limit)" : "") *
    (default === nothing ? "" : " DEFAULT $default ") *
    (not_null ? " NOT NULL " : "") *
    " " * string(options)
end


function SearchLight.Migration.column_id(name::Union{String,Symbol} = "id", options::Union{String,Symbol} = "") :: String
  "$name INTEGER PRIMARY KEY $options"
end


function SearchLight.Migration.add_index(table_name::Union{String,Symbol}, column_name::Union{String,Symbol}; name::Union{String,Symbol} = "", unique::Bool = false) :: Nothing
  name = isempty(name) ? SearchLight.index_name(table_name, column_name) : name
  SearchLight.query("CREATE $(unique ? "UNIQUE" : "") INDEX $(name) ON $table_name ($column_name)", internal = true)

  nothing
end


function SearchLight.Migration.add_column(table_name::Union{String,Symbol}, name::Union{String,Symbol}, column_type::Union{String,Symbol}; default::Union{String,Symbol,Nothing} = nothing, limit::Union{Int,Nothing} = nothing, not_null::Bool = false) :: Nothing
  SearchLight.query("ALTER TABLE $table_name ADD $(SearchLight.Migration.column(name, column_type, default = default, limit = limit, not_null = not_null))", internal = true)

  nothing
end


function SearchLight.Migration.drop_table(name::Union{String,Symbol}) :: Nothing
  SearchLight.query("DROP TABLE $name", internal = true)

  nothing
end


function SearchLight.Migration.remove_index(name::Union{String,Symbol}) :: Nothing
  SearchLight.query("DROP INDEX $name", internal = true)

  nothing
end


#### GENERATOR ####


function SearchLight.Generator.FileTemplates.adapter_default_config()
  """
  $(SearchLight.config.app_env):
    adapter:  SQLite
    filename: db/$(SearchLight.config.app_env).sqlite3
  """
end

end
