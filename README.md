# NetMap

## Demo

[![asciicast](https://asciinema.org/a/d5DW80hzdN1fOBn8azBYZIkan.svg)](https://asciinema.org/a/d5DW80hzdN1fOBn8azBYZIkan)

**select 1 Country, filter Location NE Asia**
![Example 1](./examples/1.png)

**select 2 City, filter Location EQ Europe**
![Example 2](./examples/2.png)

## Description
This is REPL for interacting with netmap in NEOFS and applying placement rules to it.
Netmap and CRUSH enchacement with FILTERs is described in research plan.

## Commands
To see help for specific command type `command help`.

### help
`help`

Get a list of commands.

### exit
`exit`

Exit program.

### load
`load <filename>`

Load netmap from specified file.

### save
`save <filename>`

Save netmap to specified file.

### clear
`clear`

Clear current netmap.

### select
`select <number> <key>`

Example:
```
>>> add 1 /Location:Europe/Country:Germany
>>> add 2 /Location:Europe/Country:Austria
>>> add 3 /Location:Asia/Country:Korea
>>> add 4 /Location:Asia/Country:Japan
>>> select 1 Location
>>> select 2 Country
```


### filter
`filter <key> <operation> <value>`

Operation can be one of EQ, NE, LT, LE, GT, GE

Example:
```
>>> add 1 /Location:Europe/Country:Germany
>>> add 2 /Location:Europe/Country:Austria
>>> filter Country NE Austria
```


### get-selection
`get-selection`

Get nodes from current selection.

Example:
```
>>> load /examples/map2
>>> select 1 Country
>>> filter Location NE Asia
>>> get-selection
[13 14]
```

### clear-selection
`clear-selection`

Clear current selection.

### dump-selection
`dump-selection <filename>`

Dump netmap in graphical format. If using docker, `/pics` directory is mounted as `temp` on host.

