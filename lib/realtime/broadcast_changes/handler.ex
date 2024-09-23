defmodule Realtime.BroadcastChanges.Handler do
  use GenServer
  require Logger

  import Realtime.Adapters.Postgres.Protocol
  import Realtime.Adapters.Postgres.Decoder
  import Realtime.Helpers, only: [log_error: 2]

  alias Realtime.Adapters.Postgres.Decoder
  alias Realtime.Adapters.Postgres.Protocol.KeepAlive
  alias Realtime.Adapters.Postgres.Protocol.Write
  alias Realtime.Api.Tenant
  alias Realtime.BroadcastChanges.PostgresReplication
  alias Realtime.Database
  alias Realtime.Tenants.BatchBroadcast

  defstruct [:tenant_id, relations: %{}, buffer: []]

  @behaviour PostgresReplication.Handler

  def start(%Tenant{external_id: tenant_id} = tenant, opts \\ []) do
    supervisor_spec =
      {:via, PartitionSupervisor,
       {Realtime.BroadcastChanges.Handler.DynamicSupervisor, tenant_id}}

    connection_opts = Database.from_tenant(tenant, "realtime_broadcast_changes", :stop, true)
    name = {:via, Registry, {Realtime.BroadcastChanges.Handler.Registry, tenant_id}}
    opts = [tenant_id: tenant_id, connection_opts: connection_opts, name: name] ++ opts
    chidlren_spec = {__MODULE__, opts}
    Logger.info("Initializing handler for #{tenant_id}")

    case DynamicSupervisor.start_child(supervisor_spec, chidlren_spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      error ->
        log_error("UnableToStartHandler", error)
        {:error, :handler_failed_to_start}
    end
  end

  @impl true
  def call(message, metadata) when is_write(message) do
    %{tenant_id: tenant_id} = metadata
    %Write{message: message} = parse(message)

    case Registry.lookup(Realtime.BroadcastChanges.Handler.Registry, tenant_id) do
      [{pid, _}] ->
        message
        |> decode_message()
        |> then(&send(pid, &1))

      _ ->
        Logger.warning("Unable to find BroadcastChanges for tenant: #{tenant_id}")
        :ok
    end

    :noreply
  end

  def call(message, _metadata) when is_keep_alive(message) do
    %KeepAlive{reply: reply, wal_end: wal_end} = parse(message)
    wal_end = wal_end + 1

    message =
      case reply do
        :now -> standby_status(wal_end, wal_end, wal_end, reply)
        :later -> hold()
      end

    {:reply, message}
  end

  def call(msg, state) do
    Logger.info("Unknown message received: #{inspect(%{msg: parse(msg), state: state})}")
    :noreply
  end

  @impl true

  def handle_info(%Decoder.Messages.Relation{} = msg, state) do
    %Decoder.Messages.Relation{id: id, namespace: namespace, name: name, columns: columns} = msg
    %{relations: relations} = state
    relation = %{name: name, columns: columns, namespace: namespace}
    relations = Map.put(relations, id, relation)
    {:noreply, %{state | relations: relations}}
  end

  def handle_info(%Decoder.Messages.Insert{} = msg, state) do
    %Decoder.Messages.Insert{relation_id: relation_id, tuple_data: tuple_data} = msg
    %{buffer: buffer, relations: relations} = state

    case Map.get(relations, relation_id) do
      %{columns: columns} ->
        to_broadcast =
          tuple_data
          |> Tuple.to_list()
          |> Enum.zip(columns)
          |> Enum.map(fn
            {nil, %{name: name}} -> {name, nil}
            {value, %{name: name, type: "jsonb"}} -> {name, Jason.decode!(value)}
            {value, %{name: name, type: "bool"}} -> {name, value == "t"}
            {value, %{name: name}} -> {name, value}
          end)
          |> Map.new()

        topic = Map.get(to_broadcast, "topic")
        private = Map.get(to_broadcast, "private")
        event = Map.get(to_broadcast, "event")
        payload = Map.get(to_broadcast, "payload")
        id = Map.get(to_broadcast, "id")

        payload = Map.put(payload, "id", id)

        to_broadcast = %{
          topic: topic,
          event: event,
          private: private,
          payload: payload
        }

        buffer = buffer ++ [to_broadcast]
        {:noreply, %{state | buffer: buffer}}

      _ ->
        log_error("UnknownBroadcastChangesRelation", "Relation ID not found: #{relation_id}")
        {:noreply, state}
    end
  end

  def handle_info(%Decoder.Messages.Commit{}, %{buffer: []} = state) do
    {:noreply, state}
  end

  def handle_info(%Decoder.Messages.Commit{}, state) do
    %{buffer: buffer, tenant_id: tenant_id} = state
    tenant = Realtime.Tenants.Cache.get_tenant_by_external_id(tenant_id)

    case BatchBroadcast.broadcast(nil, tenant, %{messages: buffer}, true) do
      :ok -> :ok
      error -> log_error("UnableToBatchBroadcastChanges", error)
    end

    {:noreply, %{state | buffer: []}}
  end

  def handle_info(_, state), do: {:noreply, state}
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, opts)

  @impl true
  def init(opts) do
    Logger.info("Initializing connection with the status: #{inspect(opts)}")

    tenant_id = Keyword.fetch!(opts, :tenant_id)
    connection_opts = Keyword.fetch!(opts, :connection_opts)

    supervisor =
      {:via, PartitionSupervisor,
       {Realtime.BroadcastChanges.Listener.DynamicSupervisor, tenant_id}}

    name = {:via, Registry, {Realtime.BroadcastChanges.Listener.Registry, tenant_id}}

    configuration = %PostgresReplication{
      connection_opts: [
        hostname: connection_opts.host,
        username: connection_opts.user,
        password: connection_opts.pass,
        database: connection_opts.name,
        port: connection_opts.port,
        parameters: [
          application_name: connection_opts.application_name
        ]
      ],
      table: "messages",
      schema: "realtime",
      handler_module: __MODULE__,
      opts: [name: name],
      metadata: %{tenant_id: tenant_id}
    }

    children_spec = {PostgresReplication, configuration}
    state = %__MODULE__{tenant_id: tenant_id, buffer: [], relations: %{}}

    case DynamicSupervisor.start_child(supervisor, children_spec) do
      {:ok, _pid} ->
        {:ok, state}

      {:error, {:already_started, _pid}} ->
        {:ok, state}

      error ->
        log_error("UnableToStartPostgresReplication", error)
        {:stop, :shutdown}
    end
  end
end
