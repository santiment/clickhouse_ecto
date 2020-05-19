if Code.ensure_loaded?(Clickhousex) do
  defmodule ClickhouseEcto.Connection do
    alias Clickhousex.Query
    alias ClickhouseEcto.Query, as: SQL

    @behaviour Ecto.Adapters.SQL.Connection

    @typedoc "The prepared query which is an SQL command"
    @type prepared :: String.t()

    @typedoc "The cache query which is a DBConnection Query"
    @type cached :: map

    @doc """
    Receives options and returns `DBConnection` supervisor child specification.
    """
    @impl Ecto.Adapters.SQL.Connection
    def child_spec(opts) do
      DBConnection.child_spec(Clickhousex.Protocol, opts)
    end

    @doc """
    Prepares and executes the given query with `DBConnection`.
    """
    @impl Ecto.Adapters.SQL.Connection
    def prepare_execute(conn, name, prepared_query, params, options) do
      query = %Query{name: name, statement: prepared_query}
      ordered_params = order_params(prepared_query, params)

      case DBConnection.prepare_execute(conn, query, ordered_params, options) do
        {:ok, query, result} ->
          {:ok, %{query | statement: prepared_query}, process_rows(result, options)}

        {:error, %Clickhousex.Error{}} = error ->
          if is_no_data_found_bug?(error, prepared_query) do
            {:ok, %Query{name: "", statement: prepared_query}, %{num_rows: 0, rows: []}}
          else
            error
          end

        {:error, error} ->
          raise error
      end
    end

    @doc """
    Executes the given prepared query with `DBConnection`.
    """
    @impl Ecto.Adapters.SQL.Connection
    def execute(conn, %Query{} = query, params, options) do
      case DBConnection.prepare_execute(conn, query, params, options) do
        {:ok, _query, result} ->
          {:ok, process_rows(result, options)}

        {:error, %Clickhousex.Error{}} = error ->
          if is_no_data_found_bug?(error, query.statement) do
            {:ok, %{num_rows: 0, rows: []}}
          else
            error
          end

        {:error, error} ->
          raise error
      end
    end

    def execute(conn, statement, params, options) do
      execute(conn, %Query{name: "", statement: statement}, params, options)
    end

    @impl Ecto.Adapters.SQL.Connection
    def query(conn, statement, params, opts) do
      Clickhousex.query(conn, statement, params, opts)
    end

    @impl Ecto.Adapters.SQL.Connection
    def stream(_conn, _statement, _params, _opts) do
      raise("stream not implemented for Clickhouse")
    end

    @impl Ecto.Adapters.SQL.Connection
    def to_constraints(_error), do: []

    ## Queries
    @impl Ecto.Adapters.SQL.Connection
    def all(query) do
      SQL.all(query)
    end

    @impl Ecto.Adapters.SQL.Connection
    def update_all(query, prefix \\ nil), do: SQL.update_all(query, prefix)

    @impl Ecto.Adapters.SQL.Connection
    def delete_all(query), do: SQL.delete_all(query)

    @impl Ecto.Adapters.SQL.Connection
    def insert(prefix, table, header, rows, on_conflict, returning),
      do: SQL.insert(prefix, table, header, rows, on_conflict, returning)

    @impl Ecto.Adapters.SQL.Connection
    def update(prefix, table, fields, filters, returning),
      do: SQL.update(prefix, table, fields, filters, returning)

    @impl Ecto.Adapters.SQL.Connection
    def delete(prefix, table, filters, returning),
      do: SQL.delete(prefix, table, filters, returning)

    ## Migration
    @impl Ecto.Adapters.SQL.Connection
    def execute_ddl(command), do: ClickhouseEcto.Migration.execute_ddl(command)

    @impl Ecto.Adapters.SQL.Connection
    def ddl_logs(_result), do: raise("ddl_logs not implemented for Clickhouse")

    @impl Ecto.Adapters.SQL.Connection
    def table_exists_query(table) do
      {"SELECT 1 FROM system.tables WHERE name = ?1 LIMIT 1", [table]}
    end

    # Private functions

    defp is_no_data_found_bug?({:error, error}, statement) do
      is_dml =
        statement
        |> IO.iodata_to_binary()
        |> (fn string ->
              String.starts_with?(string, "INSERT") || String.starts_with?(string, "DELETE") ||
                String.starts_with?(string, "UPDATE")
            end).()

      is_dml and error.message =~ "No SQL-driver information available."
    end

    defp process_rows(result, options) do
      decoder = options[:decode_mapper] || fn x -> x end

      Map.update!(result, :rows, fn row ->
        unless is_nil(row), do: Enum.map(row, decoder)
      end)
    end

    defp order_params(query, params) do
      sanitised =
        Regex.replace(~r/(([^\\]|^))["'].*?[^\\]['"]/, IO.iodata_to_binary(query), "\\g{1}")

      ordering =
        Regex.scan(~r/\?([0-9]+)/, sanitised)
        |> Enum.map(fn [_, x] -> String.to_integer(x) end)

      ordering_count = Enum.max_by(ordering, fn x -> x end, fn -> 0 end)

      if ordering_count != length(params) do
        raise "\nError: number of params received (#{length(params)}) does not match expected (#{
                ordering_count
              })"
      end

      ordered_params =
        ordering
        |> Enum.reduce([], fn ix, acc -> [Enum.at(params, ix - 1) | acc] end)
        |> Enum.reverse()

      case ordered_params do
        [] -> params
        _ -> ordered_params
      end
    end
  end
end
