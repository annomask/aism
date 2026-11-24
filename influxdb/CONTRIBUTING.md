# Upstream build instructions

## How to build InfluxDB from source

### Install Go

Use the Go version specified in `go.mod`.

At InfluxData we find `gvm`, a Go version manager, useful for installing Go.
For instructions on how to install it see [the gvm page on github](https://github.com/moovweb/gvm).

After installing `gvm` you can install and set the default Go version by running the following:

```bash
$ gvm install go1.18
$ gvm use go1.18 --default
```

InfluxDB requires Go module support. Set `GO111MODULE=on` or build the project outside of your `GOPATH` for it to succeed. For information about modules, please refer to the [wiki](https://github.com/golang/go/wiki/Modules).

### Install revision control systems

Go has the ability to import remote packages via revision control systems with the `go get` command.
To ensure that you can retrieve any remote package, install `git` and `bzr` revision control software, following the instructions for your system:

- [Install Git](http://git-scm.com/book/en/Getting-Started-Installing-Git)
- [Install Bazaar](http://doc.bazaar.canonical.com/latest/en/user-guide/installing_bazaar.html)

### Install additional dependencies

In addition to `go`, `git`, and `bzr`, you will need the following prerequisites
installed on your system:

- Rust (a recent stable version, 1.60 or higher).
  To install Rust, we recommend using [rustup](https://rustup.rs/).
- `clang`
- `make`
- `pkg-config`
- `protobuf`
- Go protobuf plugin. To use Go to install the plugin, enter the following command in your terminal:

  `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28`

To install prerequisites, use the following example command for your system:

- OSX: `brew install pkg-config protobuf`
  - For OSX, you must have [HomeBrew](https://brew.sh) installed.
  - You will also need the [Developer Tools](https://webkit.org/build-tools/), which includes `make`.
- Linux (Arch): `pacman -S clang make pkgconf protobuf`
- Linux (Ubuntu): `sudo apt install make clang pkg-config protobuf-compiler libprotobuf-dev build-essential`
- Linux (RHEL): see the [RedHat-specific instructions](#redhat-specific-instructions).

#### RedHat-specific instructions

For RedHat, you must enable the [EPEL](https://fedoraproject.org/wiki/EPEL)

### Build influxd with make

`influxd` is the InfluxDB service.

For `influx`, the InfluxDB CLI tool, see the [influx-cli repository on Github](https://github.com/influxdata/influx-cli).

Once you've installed the dependencies,
follow these steps to build `influxd` from source and start the service:

1. Clone this repo (influxdb).
2. In your influxdb directory, run `make` to generate the influxd binary:

   ```sh
   make
   ```

   If successful,  `make` installs the binary to a platform-specific path for your system.
   The output is the following:

   ```sh
   env GO111MODULE=on go build -tags 'assets ' -o bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd ./cmd/influxd
   ```

3. To start the `influxd` service that runs InfluxDB, enter the following command
   to run the platform-specific binary:

   ```
   bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd
   ```

   `influxd` logs to `stdout` by default.

**Troubleshooting**

- If you've changed Go or Rust versions and have trouble building, try running `go clean -r -x -cache -testcache -modcache ./` to clear out old build artifacts that may be incompatible.

### Run tests

This project is built from various languages.
To run tests for all languages and components, enter the following command in
your terminal:

```bash
make test
```

To run tests for only Go and Rust components, enter the following command in your terminal:

```bash
make test-go
```

## Generated Google Protobuf code

Most changes to the source don't require changes to the generated protocol buffer code.
If you need to modify the protocol buffer code, you'll first need to install the protocol buffers toolchain.

First install the [protocol buffer compiler](https://developers.google.com/protocol-buffers/) 3.17.3 or later for your OS.

Then run `go generate` after updating any `*.proto` file:

```bash
go generate ./...
```

**How to troubleshoot protobuf**

If generating the protobuf code is failing for you, check each of the following:

- Ensure the protobuf library can be found. Make sure that `LD_LIBRARY_PATH` includes the directory in which the library `libprotoc.so` has been installed.
- Ensure the command `protoc-gen-go`, found in `GOPATH/bin`, is on your path. This can be done by adding `GOPATH/bin` to `PATH`.

## Generated Go Templates

The query engine requires optimized data structures for each data type so instead of writing each implementation several times we use templates.
_Do not change code that ends in a `.gen.go` extension!_
Instead you must edit the `.gen.go.tmpl` file that was used to generate it.

Once you've edited the template file, you'll need the [`tmpl`][tmpl] utility to generate the code:

```sh
$ go get github.com/benbjohnson/tmpl
```

Then you can regenerate all templates in the project:

```sh
$ go generate ./...
```

[tmpl]: https://github.com/benbjohnson/tmpl

## Profiling

When troubleshooting problems with CPU or memory the Go toolchain can be helpful.
You can start InfluxDB with CPU and memory profiling turned on.
For example:

```bash
# start influx with profiling

$ ./influxd -cpuprofile influxdcpu.prof -memprof influxdmem.prof

# run queries, writes, whatever you're testing
# Quit out of influxd and influxd.prof will then be written.
# open up pprof to examine the profiling data.

$ go tool pprof ./influxd influxd.prof

# once inside run "web", opens up browser with the CPU graph
# can also run "web <function name>" to zoom in. Or "list <function name>" to see specific lines
```

Note that when you pass the binary to `go tool pprof` _you must specify the path to the binary_.

If you are profiling benchmarks built with the `testing` package, you may wish
to use the [`github.com/pkg/profile`](github.com/pkg/profile) package to limit
the code being profiled:

```go
func BenchmarkSomething(b *testing.B) {
  // do something intensive like fill database with data...
  defer profile.Start(profile.ProfilePath("/tmp"), profile.MemProfile).Stop()
  // do something that you want to profile...
}
```
