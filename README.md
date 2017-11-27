# Install

```bash
go get github.com/zhuangsirui/kafka-cli
```

# Examples

Simply call `kafka-cli` after install

## connect

```bash
disconnected > connect --addrs broker-1:port,broker-2:port,broker-3:port
connecting... [broker-1:port broker-2:port broker-3:port]
connected >
```

## disconnect

```bash
connected > disconnect
disconnected >
```

## list

**list topics**

```bash
connected > list topics
[topic-1 topic-2]
```

**list partitions**

```bash
connected > list partitions --topic topic-1
[0 1 2]
```

## get

**get offset**

```bash
connected > get offset --topic topic-1 --partition 0 # default for the newest offset on the partition
10
connected > get offset --topic topic-1 --partition 0 --time -2 # you can set `--time -2` to show oldest offset on the partition
0
```

## produce

```bash
connected > produce --topic test --partition 0 --key 'key for the log' --value 'value for the log'
produce successful, offset 10
```

## consume

```bash
connected > consume --topic test --partition 0
{"key":"key","value":"value","offset":0}
{"key":"key","value":"value","offset":1}
{"key":"key","value":"value","offset":2}
{"key":"key","value":"value","offset":3}
{"key":"key","value":"value","offset":4}
{"key":"key","value":"value","offset":5}
{"key":"key","value":"value","offset":6}
{"key":"key","value":"value","offset":7}
{"key":"key","value":"value","offset":8}
{"key":"key","value":"value","offset":9}
{"key":"key for the log","value":"value for the log","offset":10} # C^c to stop consume
connected >
```

You can set `--output` to assign where to print the consume data

```bash
connected > consume --topic test --partition 0 --output ./topic-test-0.json # C^c to stop consume
connected >
```

```bash
$ cat ./topic-test-0.json
{"key":"key","value":"value","offset":0}
{"key":"key","value":"value","offset":1}
{"key":"key","value":"value","offset":2}
{"key":"key","value":"value","offset":3}
{"key":"key","value":"value","offset":4}
{"key":"key","value":"value","offset":5}
{"key":"key","value":"value","offset":6}
{"key":"key","value":"value","offset":7}
{"key":"key","value":"value","offset":8}
{"key":"key","value":"value","offset":9}
{"key":"key for the log","value":"value for the log","offset":10}
```

**more flags at `consume --help`**

## non-interactive mode

You can add `--addrs` flags after `kafka-cli` and run what you want directly from shell:

```bash
$ kafka-cli --addrs broker-0:port produce --topic test --partition 0 --key 'key from bash' --value 'value from bash'
produce successful, offset 11
```

# TODO

* Make some examples on readme
* Support csv output
