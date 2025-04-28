defmodule HutchTest do
  use ExUnit.Case
  doctest Hutch

  test "greets the world" do
    assert Hutch.hello() == :world
  end
end
