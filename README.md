# eKuiper Plus - An edge lightweight IoT data analytics software based on LF Edge eKuiper and advanced modules

The open source project [eKuiper](https://github.com/lf-edge/ekuiper) is a lightweight IoT data analytics and stream
processing engine running on resource-constraint edge devices. Since v1.14.0-alpha.1, eKuiper project supports the
module system, which allows users to extend the functionalities of eKuiper by adding new modules. The module could be a
private project and do not have excessive limits like plugin. It provides a more powerful and flexible solution to
maintain **private** eKuiper version with additional advanced feature.

This repo is an integrated project based on eKuiper and advanced modules to provide the business version of eKuiper for
NeuronEX.

## Current Modules

The following modules are included in this project:

- [can](https://github.com/emqx/ekuiper_can): provides the CAN related features for eKuiper

## How to Add Module

Firstly, you should prepare a module project and write the module according to the eKuiper module requirement. Then add
the module and optional FVT to this project.

### Write the Module

1. Create a new module project in GitHub (public or private) repo.
2. Write your module implementation and unit tests.
3. Write the module init code in the root directory of the module project. Use eKuiper's module register functions to
   register the module source, sink, converter, etc.
    ```go
    package ekuiper_can
  
    func init() {
      modules.RegisterSource("can", can.GetSource)
      modules.RegisterConverter("can", func(schemaFile string, _ string, _ string) (message.Converter, error) {
        return converterCan.NewConverter(schemaFile)
      })
      modules.RegisterConverter("canjson", func(schemaFile string, _ string, _ string) (message.Converter, error) {
        return canjson.NewConverter(schemaFile)
      })
    }
    ```

For full example, please refer to the [can module](https://github.com/emqx/ekuiper_can).

### Add Module

1. In the `main.go` of this project, import the above module anonymously, like the ekuiper_can below. This will trigger
   the module init function to register the modules.
    ```go
    import (
      "github.com/lf-edge/ekuiper/cmd"
    
      _ "github.com/emqx/ekuiper_can"
    )
    ```
2. Make sure eKuiper and the module are in the `go.mod` file.
   ```go
   require (
	    github.com/emqx/ekuiper_can v1.0.0
	    github.com/lf-edge/ekuiper v1.14.0-dev.1
   )
   ```
3. Run `go mod tidy` to update the dependencies. If the module is private and cannot be downloaded, try to
   run `go env -w GOPRIVATE=github.com/emqx/ekuiper_can` and try again.
4. Add any FVT tests to `test` directory.

### Update Versions

Basically, unless the eKuiper module API changes, the module should be able to integrate with any version of eKuiper.

To update the eKuiper or module version, you can modify the `go.mod` file directly or use the `go get` command.

### How to update eKuiper_can

```shell
GOPRIVATE=github.com/emqx/ekuiper_can  go get github.com/emqx/ekuiper_can@<commit-id>
```