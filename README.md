## Index - a platform-agnostic multi-level index

[![OCaml-CI Build Status](https://img.shields.io/endpoint?url=https%3A%2F%2Fci.ocamllabs.io%2Fbadge%2Fmirage%2Findex%2Fmaster&logo=ocaml)](https://ci.ocamllabs.io/github/mirage/index)
[![Build Status](https://travis-ci.org/mirage/index.svg?branch=master)](https://travis-ci.org/mirage/index)

Index is a scalable implementation of persistent indices in OCaml.

It takes an arbitrary IO implementation and user-supplied content types 
and supplies a standard key-value interface for persistent storage. 
Index provides instance sharing by default: 
each OCaml run-time shares a common singleton instance.

Index supports multiple-reader/single-writer access.
Concurrent access is safely managed using lock files.
