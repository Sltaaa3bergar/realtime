defmodule Realtime.BroadcastChanges.HandlerTest do
  # async: false due to the fact that we're using the database to intercept messages created which will interfer with other tests
  use Realtime.DataCase, async: false

  import ExUnit.CaptureLog
  import Mock

  alias Realtime.Api.Message
  alias Realtime.BroadcastChanges.Handler
  alias Realtime.Database
  alias Realtime.Tenants.BatchBroadcast
  alias Realtime.Tenants.Migrations

  setup do
    start_supervised(Realtime.Tenants.CacheSupervisor)
    tenant = tenant_fixture()
    [%{settings: settings} | _] = tenant.extensions
    migrations = %Migrations{tenant_external_id: tenant.external_id, settings: settings}
    Migrations.run_migrations(migrations)
    :ok
  end

  test "start/1 fails if tenant connection is invalid" do
    tenant =
      tenant_fixture(%{
        "extensions" => [
          %{
            "type" => "postgres_cdc_rls",
            "settings" => %{
              "db_host" => "localhost",
              "db_name" => "postgres",
              "db_user" => "supabase_admin",
              "db_password" => "bad",
              "db_port" => "5433",
              "poll_interval" => 100,
              "poll_max_changes" => 100,
              "poll_max_record_bytes" => 1_048_576,
              "region" => "us-east-1",
              "ssl_enforced" => false
            }
          }
        ]
      })

    capture_log(fn ->
      assert {:error, :handler_failed_to_start} = Handler.start(tenant)
    end) =~ "UnableToStartHandler"
  end

  test_with_mock "start/1 starts a handler for the tenant and broadcasts for single insert",
                 BatchBroadcast,
                 broadcast: fn _, _, _, _ -> :ok end do
    tenant = tenant_fixture()
    assert {:ok, pid} = Handler.start(tenant)
    Process.link(pid)
    total_messages = 5
    # Works with one insert per transaction
    for _ <- 1..total_messages do
      message_fixture(tenant, %{
        "topic" => random_string(),
        "private" => true,
        "event" => "INSERT",
        "payload" => %{"value" => random_string()}
      })
    end

    :timer.sleep(1000)
    assert_called_exactly(BatchBroadcast.broadcast(nil, tenant, :_, :_), total_messages)
    # Works with batch inserts
    messages =
      for _ <- 1..total_messages do
        Message.changeset(%Message{}, %{
          "topic" => random_string(),
          "private" => true,
          "event" => "INSERT",
          "payload" => %{"value" => random_string()}
        })
      end

    Database.connect(tenant, "realtime_test", 1)
    Realtime.Repo.insert_all_entries(Message, messages, Message)
    :timer.sleep(1000)
    assert_called_exactly(BatchBroadcast.broadcast(nil, tenant, :_, :_), total_messages)

    on_exit(fn ->
      {:ok, conn} = Realtime.Database.connect(tenant, "realtime_test", 1)

      {:ok, _} =
        Postgrex.query(conn, "DROP PUBLICATION realtime_messages_publication", [])

      Realtime.Database.replication_slot_teardown(tenant)
    end)
  end
end
