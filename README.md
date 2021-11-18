# ALF
[![Hex.pm](https://img.shields.io/hexpm/v/alf.svg?style=flat-square)](https://hex.pm/packages/alf)
## Flow-Based Application Layer Framework

#### ALF is a set of abstractions built on top Elixir GenStage which allows writing program following [Flow-Based Programming](https://en.wikipedia.org/wiki/Flow-based_programming) approach.
#### ALF is a successor of the [Flowex](https://github.com/antonmi/flowex) project. Check its Readme to get the general idea. ALF adds conditional branching, packet cloning, goto statement, and other functionalities. Therefore, one can create application trees (graphs) of arbitrary complexity. 

## Installation
Just add `:alf` as dependency to the `mix.exs` file.
ALF starts its own supervisor (`ALF.DynamicSupervisor`). All the pipelines and managers are started under the supervisor

## Quick start
Read a couple of sections of [Flowex README](https://github.com/antonmi/flowex#readme) to get the basic idea.
### Define your pipeline
```elixir
defmodule ThePipeline do
  use ALF.DSL

  @components [
    stage(:add_one),
    stage(:mult_by_two),
    stage(:minus_three)
  ]

  def add_one(datum, _opts), do: datum + 1
  def mult_by_two(datum, _opts), do: datum * 2
  def minus_three(datum, _opts), do: datum - 3
end
```
### Start the pipeline
```elixir
:ok = ALF.Manager.start(ThePipeline)
```
This starts a manager (GenServer) with `ThePipeline` name. The manager starts all the components and puts them under another DynamicSupervisor supervision tree.

### Use the pipeline
The only interface currently is the `stream_to` function (`stream_to/2` and `stream_to/3`).
It receives a stream or `Enumerable.t` and returns another stream where results will be streamed.
```elixir
inputs = [1,2,3]
output_stream =  Manager.stream_to(inputs, Pipeline)
Enum.to_list(output_stream) # it returns [1, 3, 5]
```

Check [test/examples](https://github.com/antonmi/ALF/tree/main/test/examples) folder for more examples


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



## Components / Pipeline reusing


### `stages_from` macro
One can easily include components from another pipeline:
```elixir
defmodule ReusablePipeline do
  use ALF.DSL
  @components [
    stage(:foo),
    stage(:bar)
  ]
end

defmodule ThePipeline do
  use ALF.DSL
  @components stages_from(ReusablePipeline) ++ [stage(:baz)]
end
```

### `plug_with` macro
Use the macro if you include other components that expect different type/format/structure of input data.
```elixir
defmodule ThePipeline do
  use ALF.DSL
  
  @components [
                plug_with(AdapterModuleBaz, do: [stage(:foo), stage(:bar)])
              ] ++
                plug_with(AdapterModuleForReusablePipeline) do
                  stages_from(ReusablePipeline)
                end
   
end
```
`plug_with` adds `Plug` component before the components in the block and `Unplug` at the end.
The first argument is an "adapter" module which must implement the `plug/2` and `unplug/3` functions
```elixir
def AdapterModuleBaz do
  def init(opts), do: opts # optional
  
  def plug(datum, _opts) do
    # the function is called inside the `Plug` component 
    # `datum` will be put on the "AdapterModuleBaz" until IP has reached the "unplug"
    # the function must return `new_datum` with the structure expected by the following component
    new_datum
  end
  
  def unplug(datum, prev_datum, _opts) do
    # here one can access previous "datum" in `prev_datum`
    # transform the data back for the following components
  end
end
```













