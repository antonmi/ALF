# ALF

## Flow-Based Application Layer Framework

#### ALF is a set of abstractions built on top Elixir GenStage which allows writing program with [Flow-Based Programming](https://en.wikipedia.org/wiki/Flow-based_programming) paradigm.
#### ALF is a successor of the [Flowex](https://github.com/antonmi/flowex) project. Check its Readme to get the general idea. ALF adds conditional branching, packet cloning, and goto statement functionalities. Therefore, one can create application trees (graphs) of arbitrary complexity 

## Installation
Just add `:alf` as dependency to the `mix.exs` file.
ALF starts its own supervisor (`ALF.DynamicSupervisor`). All the pipelines and managers are started under the supervisor

## Components overview
### Stage
Stage is the main component where one puts a piece of application logic. It might be a simple 2-arity function or a module with `call/2` function:
```elixir
  stage(:my_fun, opts: %{foo: bar})
  # or
  stage(:MyComponent, opts: %{})
```
where `MyComponent` is
```elixir
defmodule MyComponent do
  # optional
  def init(opts), do: %{opts | foo: :bar}
  
  def call(datum, opts) do
    # logic is here
    new_datum
  end
end
```

### Switch
Switch allows to forward IP (information packets) to different branches:
```elixir
switch(:my_switch,
        branches: %{
          part1: [stage(:foo)],
          part2: [stage(:bar)]
        },
        cond: :cond_function
        opts: [foo: :bar]
      )
```
The `cond` function is 2-arity function that must return the key of the branch:
```elixir
def cond_function(datum, opts) do
  if datum == opts[:foo], do: :part1, else: part: 2
end
```

### Clone
Clones an IP, useful for background actions.
```elixir
clone(:my_clone, to: [stage(:foo), dead_end(:dead_end)])
```

### Goto
Send packet to a given `goto_point`
```elixir
goto(:my_goto, to: :my_goto_point, if: :goto_function, opts: [foo: :bar])
```
The `if` function is 2-arity function that must return `true` of `false`
```elixir
def goto_function(datum, opts) do
  datum == opts[:foo]
end
```

### GotoPoint
The `Goto` component companion
```elixir
goto_point(:goto_point)
```
### DeadEnd
IP won't propagate further. It's used alongside with the `Clone` component to avoid duplicate IPs in output
```elixir
dead_end(:dead_end)
```





