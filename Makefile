.PHONY: all clean test fuzz doc examples

all:
	dune build

test:
	dune runtest

fuzz:
	dune build @fuzz --no-buffer

simple-crowbar:
	dune build fuzz/main.exe
	dune exec fuzz/main.exe

examples:
	dune build @examples

clean:
	dune clean

doc:
	dune build @doc
