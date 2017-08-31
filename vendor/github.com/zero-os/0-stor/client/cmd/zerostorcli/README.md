# Simple 0-stor client cli

Simple cli to store file to 0-stor.

## Installation

```
 go get -u github.com/zero-os/0-stor/client/cmd/zerostorcli
 ```

## Configuration

- By default, client uses current ```config.yaml``` file in same dir
./- If you want to use another file use ```zerostorcli --conf newFile```

- Update the client config.yaml file in the same directory
    - Update the organization & namespace to match existing IYO ```{organization}.0stor.{namespace}```
    - Update ```iyo_client_id``` & ```iyo_secret```
    - Update ```meta_shards``` URL to match URL of existing etcd server
    - Update ```shards``` URLs to match existing 0stor server instances.
        - **Warning** : ALWAYS USE DIFFERENT 0STOR INSTANCES AS SHARDS (NOT ONE INSTANCE)

## Commands
The CLI expose two group of commands, file and namespace. Each group contains sub commands.

- file
  - upload: Upload a file to the 0-stor(s)
  - download: Download a file from the 0-stor(s)
  - metadata: Print the metadata of a key
- namespace
  - create: Create a namespace by creation the required sub-ogranization on [ItsYou.Online](https://itsyou.online/)
  - delete: Delete a namespace by deleting the sub-organizations.
  - get-acl: Print the permission of a user for a namepace
  - set-acl: Set the permission on a namespace for a user


### Example

The `config.yaml` used in this example has 4 pipes defines for data processing.

- First pipe chunk file into smaller blocks
- Second pipe compress all the blocks using snappy
- Third pipe encrypt the blocks
- Fourth pipe apply erasure coding on all the blocks.

#### Upload file

```
./zerostorcli --conf conf_file file upload file_name
```

upload file and use the `file_name` as 0-stor key

`conf_file` is the configuration file. See `config.yaml` here for the example

#### Download file
```
./zerostorcli --conf conf_file file download key result_file_name
```

Get value with key=`key` from 0-stor server and write it to `result_file_name`

`conf_file` is the configuration file. See `config.yaml` here for the example


#### Read metadata of a file

```
./zerostocli --conf config_file file metadata key | json_pp
```
This will print the metadata of the object with the key `key` and pretty print the json output


#### Create a namespace

```
./zerostorcli --conf conf_file namespace create namespace_test
```

This command will create the required sub-organization on ItsYou.online.
This command used the `organization` field from the configuration file. If the organization is set to `myorg` the create sub-org will be :
- `myorg.0stor.namespace_test`
- `myorg.0stor.namespace_test.read`
- `myorg.0stor.namespace_test.write`
- `myorg.0stor.namespace_test.delete`

#### Delete a namespace
```
./zerostorcli --conf conf_file namespace delete namespace_test
```

This command will delete the organization `myorg.0stor.namespace_test` and all its sub-organization

#### Set rights of a user into a namespace

```shell
./zerostorcli --conf conf_file namespace set-acl --namespace namespace_test --user johndoe -r -w -d
```

This command will authorize the user `johndoe` into the namespace `namespace_test` with the right read, write and delete.

The different rights that can be set are:
- read
- write
- delete
- admin (has all the right plus can inspect namespaces stats and resrevations)

Note that once you give access to a user, he will receive an invitation from ItsYou.online to join the orgnanization. He needs to accept the invitaion before beeing able to access the 0-stor(s).


To remove some right, just re-execute the command with the new rights. if no right are passed, then the user is unauthorized.


#### Get the right of a user

```shell
./zerostorcli --conf demo.yaml namespace get-acl --namespace namespace_test --user johndoe
```

This command will the right that a user has for the specified namespace:

```
User johndoe :
Read: true
Write: true
Delete: true
Admin: true
```
