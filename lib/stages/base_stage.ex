defmodule ALF.BaseStage do
  defmacro __using__(_opts) do
    quote do
      use GenStage

      def __state__(pid) when is_pid(pid) do
        GenStage.call(pid, :__state__)
      end

      def handle_call(:__state__, _from, state) do
        {:reply, state, [], state}
      end

      def subscribers(pid) do
        GenStage.call(pid, :subscribers)
      end

      def handle_call(:subscribers, _form, state) do
        {:reply, state.subscribers, [], state}
      end

      def handle_subscribe(:consumer, subscription_options, from, state) do
        subscribers = [from | state.subscribers]
        {:automatic, %{state | subscribers: subscribers}}
      end

      def handle_subscribe(:producer, subscription_options, from, state) do
        {:automatic, state}
      end
    end
  end
end
