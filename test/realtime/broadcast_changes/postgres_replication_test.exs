defmodule Realtime.BroadcastChanges.PostgresReplicationTest do
  # async: false due to usage of tenant database
  use Realtime.DataCase, async: false

  alias Realtime.BroadcastChanges.PostgresReplication
  alias Realtime.Database

  import Realtime.Adapters.Postgres.Decoder
  import Realtime.Adapters.Postgres.Protocol

  defmodule Handler do
    @behaviour PostgresReplication.Handler
    def call(message, %{pid: pid}) when is_write(message) do
      %{message: message} = parse(message)

      message
      |> decode_message()
      |> then(&send(pid, &1))

      :noreply
    end

    def call(message, _) when is_keep_alive(message) do
      %{reply: reply, wal_end: wal_end} = parse(message)
      wal_end = wal_end + 1

      message =
        case reply do
          :now -> standby_status(wal_end, wal_end, wal_end, reply)
          :later -> hold()
        end

      {:reply, message}
    end
  end

  describe "able to connect sucessfully" do
    setup do
      tenant = tenant_fixture()
      connection_opts = Database.from_tenant(tenant, "realtime_broadcast_changes", :stop, true)

      config = %PostgresReplication{
        connection_opts: [
          hostname: connection_opts.host,
          username: connection_opts.user,
          password: connection_opts.pass,
          database: connection_opts.name,
          port: connection_opts.port
        ],
        schema: "realtime",
        table: "messages",
        handler_module: __MODULE__.Handler,
        metadata: %{pid: self()}
      }

      start_supervised!({PostgresReplication, config})

      on_exit(fn ->
        {:ok, conn} = Database.connect(tenant, "realtime_test", 1)
        pub = PostgresReplication.publication_name(config)
        {:ok, _} = Postgrex.query(conn, "DROP PUBLICATION #{pub}", [])
        Realtime.Database.replication_slot_teardown(tenant)
      end)

      {:ok, %{tenant: tenant}}
    end

    test "handles messages for the given replicated table", %{tenant: tenant} do
      # Emit message to be captured by Handler
      message_fixture(tenant)
      assert_receive %Realtime.Adapters.Postgres.Decoder.Messages.Begin{}
      assert_receive %Realtime.Adapters.Postgres.Decoder.Messages.Relation{}
      assert_receive %Realtime.Adapters.Postgres.Decoder.Messages.Insert{}
      assert_receive %Realtime.Adapters.Postgres.Decoder.Messages.Commit{}
    end
  end

  describe "unable to connect sucessfully" do
    test "process does not start" do
      config = %PostgresReplication{
        connection_opts: [
          hostname: "localhost",
          username: "bad",
          password: "bad",
          database: "bad"
        ],
        schema: "realtime",
        table: "messages",
        handler_module: __MODULE__.Handler,
        metadata: %{pid: self()}
      }

      result = start_supervised({PostgresReplication, config})
      assert {:error, _} = result
    end
  end
end
