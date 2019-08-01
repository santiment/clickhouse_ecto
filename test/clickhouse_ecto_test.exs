defmodule ClickhouseEctoTest do
  use ExUnit.Case
  doctest ClickhouseEcto

  import Ecto.Query

  alias ClickhouseEcto.Connection, as: SQL

  defmodule Schema do
    use Ecto.Schema

    schema "test" do
      field(:app_id, :integer)
      field(:country_id, :integer)
      field(:android_id, :string)
    end
  end
end
