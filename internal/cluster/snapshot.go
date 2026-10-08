package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

func MarshalSnapshot(snap Snapshot, nodes []TopoNode) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "epoch %d\n", snap.Epoch)
	ids := make([]string, len(nodes))
	for i := range nodes {
		ids[i] = nodes[i].ID
	}
	for slot, idx := range snap.Owner {
		id := "-"
		if idx >= 0 && idx < len(ids) {
			id = ids[idx]
		}
		fmt.Fprintf(&buf, "owner %d %s\n", slot, id)
	}
	for slot, id := range snap.Migrating {
		fmt.Fprintf(&buf, "migrating %d %s\n", slot, id)
	}
	for slot, id := range snap.Importing {
		fmt.Fprintf(&buf, "importing %d %s\n", slot, id)
	}
	return buf.Bytes()
}

func UnmarshalSnapshot(data []byte, nodes []TopoNode) (Snapshot, error) {
	var snap Snapshot
	for i := range snap.Owner {
		snap.Owner[i] = -1
	}
	byID := make(map[string]int, len(nodes))
	for i := range nodes {
		byID[nodes[i].ID] = i
	}
	var seenEpoch bool
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "epoch":
			if len(fields) != 2 {
				return Snapshot{}, fmt.Errorf("bad epoch line")
			}
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return Snapshot{}, fmt.Errorf("bad epoch: %w", err)
			}
			snap.Epoch = n
			seenEpoch = true
		case "owner", "migrating", "importing":
			if len(fields) != 3 {
				return Snapshot{}, fmt.Errorf("bad %s line", fields[0])
			}
			slot, err := strconv.Atoi(fields[1])
			if err != nil || slot < 0 || slot >= NumSlots {
				return Snapshot{}, fmt.Errorf("bad slot %q", fields[1])
			}
			switch fields[0] {
			case "owner":
				if fields[2] == "-" {
					snap.Owner[slot] = -1
					continue
				}
				idx, ok := byID[fields[2]]
				if !ok {
					return Snapshot{}, fmt.Errorf("unknown node %q", fields[2])
				}
				snap.Owner[slot] = idx
			case "migrating":
				if snap.Migrating == nil {
					snap.Migrating = map[int]string{}
				}
				snap.Migrating[slot] = fields[2]
			case "importing":
				if snap.Importing == nil {
					snap.Importing = map[int]string{}
				}
				snap.Importing[slot] = fields[2]
			}
		default:
			return Snapshot{}, fmt.Errorf("bad line %q", sc.Text())
		}
	}
	if err := sc.Err(); err != nil {
		return Snapshot{}, err
	}
	if !seenEpoch {
		return Snapshot{}, fmt.Errorf("missing epoch")
	}
	return snap, nil
}
