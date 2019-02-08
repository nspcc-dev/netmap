package main

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/nspcc-dev/netmap"
	"github.com/pkg/errors"
	"gopkg.in/abiosoft/ishell.v2"
)

type state struct {
	b  *netmap.Bucket
	ss []netmap.Select
	fs []netmap.Filter
}

const stateKey = "state"

var (
	errWrongFormat = errors.New("wrong command format")
	defaultSource  = []byte("default-source-of-bytes")
)

var commands = []*ishell.Cmd{
	{
		Name: "get-selection",
		Help: "apply current selection rules",
		LongHelp: `Usage: get-selection

Example:
>>> load /examples/map2
>>> select 1 Country
>>> filter Location NE Asia
>>> get-selection
[13 14]`,
		Func: getSelection,
	},
	{
		Name:     "clear-selection",
		Help:     "clear selection rules",
		LongHelp: "Usage: clear-selection",
		Func:     clearSelection,
	},
	{
		Name:     "dump-selection",
		Help:     "dump selection result in *.dot format",
		LongHelp: "Usage: dump-selection <filename>",
		Func:     dumpNetmap,
	},
	{
		Name:     "clear",
		Help:     "clear netmap",
		LongHelp: "Usage: clear",
		Func:     clearNetmap,
	},
	{
		Name:     "load",
		Help:     "load netmap from file",
		LongHelp: "Usage: load <filename>",
		Func:     loadFromFile,
	},
	{
		Name:     "save",
		Help:     "save netmap to file",
		LongHelp: "Usage: save <filename>",
		Func:     saveToFile,
	},
	{
		Name: "add",
		Help: "add node to netmap",
		LongHelp: `Usage: add <number> /key1:value1/key2:value2 [option2 [...]]

Example:
>>> add 1 /Location:Europe/Country:Germany /Trust:10
>>> add 2 /Location:Europe/Country:Austria`,
		Func: addNode,
	},
	{
		Name: "select",
		Help: "add SELECT placement rule",
		LongHelp: `Usage: select <number> <key>

Example:
>>> add 1 /Location:Europe/Country:Germany
>>> add 2 /Location:Europe/Country:Austria
>>> add 2 /Location:Asia/Country:Korea
>>> add 2 /Location:Asia/Country:Japan
>>> select 1 Location
>>> select 2 Country`,
		Func: addSelect,
	},
	{
		Name: "filter",
		Help: "add FILTER placement rule",
		LongHelp: `Usage: filter <key> <operation> <value>
Operation can be one of EQ, NE, LT, LE, GT, GE

Example:
>>> add 1 /Location:Europe/Country:Germany
>>> add 2 /Location:Europe/Country:Austria
>>> filter Country NE Austria
`,
		Func: addFilter,
	},
	{
		Name: "spew",
		Func: func(c *ishell.Context) {
			spew.Dump(getState(c))
		},
	},
}

func main() {
	var (
		st = &state{
			b:  new(netmap.Bucket),
			ss: nil,
			fs: nil,
		}
		shell = ishell.New()
	)

	shell.Set(stateKey, st)
	for _, c := range commands {
		shell.AddCmd(c)
	}

	shell.Run()
}

func getState(c *ishell.Context) *state {
	return c.Get(stateKey).(*state)
}

func read(b *netmap.Bucket, name string) error {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return err
	}
	return b.UnmarshalBinary(data)
}

func write(b *netmap.Bucket, name string) error {
	data, err := b.MarshalBinary()
	if err != nil {
		return err
	}
	return ioutil.WriteFile(name, data, os.ModePerm)
}

func getSelection(c *ishell.Context) {
	s := getState(c)
	if b := s.b.GetMaxSelection(s.ss, s.fs); b != nil {
		if b = b.GetSelection(s.ss, defaultSource); b != nil {
			c.Println(b.Nodelist())
			return
		}
	}
	c.Println(nil)
}

func clearSelection(c *ishell.Context) {
	s := getState(c)
	s.ss = nil
	s.fs = nil
}

func dumpNetmap(c *ishell.Context) {
	if len(c.Args) != 1 {
		c.Err(errWrongFormat)
		return
	}
	s := getState(c)
	if len(s.fs) == 0 && len(s.ss) == 0 {
		if err := s.b.Dump(c.Args[0]); err != nil {
			c.Err(err)
			return
		}
		if err := dotToPng(c.Args[0], c.Args[0]+".png"); err != nil {
			c.Err(err)
			return
		}
	}
	if b := s.b.GetMaxSelection(s.ss, s.fs); b != nil {
		if b = b.GetSelection(s.ss, defaultSource); b != nil {
			if err := s.b.DumpWithSelection(c.Args[0], *b); err != nil {
				c.Err(err)
				return
			}
			if err := dotToPng(c.Args[0], c.Args[0]+".png"); err != nil {
				c.Err(err)
				return
			}
		}
	}
}

func clearNetmap(c *ishell.Context) {
	s := getState(c)
	s.b = new(netmap.Bucket)
	s.ss = nil
	s.fs = nil
}

func loadFromFile(c *ishell.Context) {
	if len(c.Args) == 0 {
		c.Err(errWrongFormat)
		return
	}
	s := getState(c)
	if err := read(s.b, c.Args[0]); err != nil {
		c.Err(err)
	}
}

func saveToFile(c *ishell.Context) {
	if len(c.Args) == 0 {
		c.Err(errWrongFormat)
		return
	}
	s := getState(c)
	if err := write(s.b, c.Args[0]); err != nil {
		c.Err(err)
	}
}

func addNode(c *ishell.Context) {
	if len(c.Args) < 2 {
		c.Err(errWrongFormat)
		return
	}
	node, err := strconv.Atoi(c.Args[0])
	if err != nil || node < 0 {
		c.Err(err)
		return
	}
	s := getState(c)
	if err = s.b.AddNode(uint32(node), c.Args[1:]...); err != nil {
		c.Err(err)
	}
}

func addSelect(c *ishell.Context) {
	if len(c.Args) != 2 {
		c.Err(errWrongFormat)
		return
	}
	count, err := strconv.ParseUint(c.Args[0], 10, 64)
	if err != nil {
		c.Err(errors.Wrapf(err, "count must be integer"))
		return
	}
	s := getState(c)
	s.ss = append(s.ss, netmap.Select{
		Key:   c.Args[1],
		Count: uint32(count),
	})
}

func addFilter(c *ishell.Context) {
	if len(c.Args) < 3 {
		c.Err(errWrongFormat)
		return
	}
	op, ok := netmap.Operation_value[c.Args[1]]
	if !ok {
		c.Err(errors.New("operation must be one of: EQ, NE, LT, LE, GT, GE"))
		return
	}
	s := getState(c)
	s.fs = append(s.fs, netmap.Filter{
		Key: c.Args[0],
		F:   netmap.NewFilter(netmap.Operation(op), c.Args[2]),
	})
}

func dotToPng(in, out string) error {
	cmd := exec.Command("dot", "-Tpng", in, "-o", out)
	return cmd.Run()
}
