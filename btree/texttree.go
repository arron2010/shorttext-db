package btree

import (
	"fmt"
	"path"
	"strings"
)

type TextNode struct {
	parent   *TextNode
	children []*TextNode
	name     string
}

const (
	verticalPipe             = '│'
	horizontalPipe           = '─'
	cornerPipe               = '└'
	verticalPipeWithOffshoot = '├'
)

// PrintAsTree prints the node & all it's children as a `tree`-style tree
func (n *TextNode) PrintAsTree() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s\n", n.name))
	n.printAsTreeHelper(&sb)
	return sb.String()
}

func (n *TextNode) printAsTreeHelper(sb *strings.Builder) {
	for _, child := range n.children {
		for _, parent := range child.findParents() {
			if parent.isRoot() {
				continue
			}

			var connChar rune
			if parent.isLastChild() {
				connChar = ' '
			} else {
				connChar = verticalPipe
			}
			sb.WriteString(fmt.Sprintf("%c%s", connChar, spaces(3)))
		}

		var connChar rune
		if child.isLastChild() {
			connChar = cornerPipe
		} else {
			connChar = verticalPipeWithOffshoot
		}

		sb.WriteString(fmt.Sprintf("%c%c%c %s\n", connChar, horizontalPipe, horizontalPipe, child.name))
		child.printAsTreeHelper(sb)
	}
}

// insert inserts all nodes represented by the supplied path. A node is added for each segment.
func (n *TextNode) insert(path string) {
	current := n
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		next := current.findChildWithName(segment)
		// if there is no node at this level with a name matching the current
		// segment, create a new node and add it as a child of "current"
		if next == nil {
			next = &TextNode{
				name:     segment,
				parent:   current,
				children: []*TextNode{},
			}
			current.children = append(current.children, next)
		}

		current = next
	}
}

func (n *TextNode) dfs(visit func(*TextNode)) {
	stack := []*TextNode{n}

	pop := func() (next *TextNode) {
		next, stack = stack[len(stack)-1], stack[:len(stack)-1]
		return next
	}

	seen := make(map[string]bool)

	for len(stack) > 0 {
		next := pop()

		// a nodes path uniquely identifies it
		p := next.printPath()
		if seen[p] {
			continue
		}

		visit(next)

		stack = append(stack, next.children...)

		seen[p] = true
	}
}

// findParents returns an array containing all the nodes parents. The parent nodes are
// returned in order of highest to lowest, and the root node is skipped. That is, the
// first node will be the parent node closest to the root.
func (n *TextNode) findParents() []*TextNode {
	parents := []*TextNode{}
	current := n
	for current.parent != nil {
		parents = append([]*TextNode{current.parent}, parents...)
		current = current.parent
	}
	return parents
}

// findChildWithName finds a child with a name matching the supplied name inside the node's
// array of child nodes
func (n *TextNode) findChildWithName(name string) *TextNode {
	for _, child := range n.children {
		if child.name == name {
			return child
		}
	}
	return nil
}

// isRoot returns true if the node has no parent
func (n *TextNode) isRoot() bool {
	return n.parent == nil
}

// isLastChild returns true if n is the final node in the parent node's array of children
func (n *TextNode) isLastChild() bool {
	if n.parent == nil {
		return false
	}
	return n.position() == len(n.parent.children)-1
}

// position returns the index of the current node in the parent node's array of children
func (n *TextNode) position() int {
	i := n.parent.indexOf(n)
	if i == -1 {
		panic("n is not a child of its parent")
	}
	return i
}

// indexOf returns the index of `target` in the `children` array of `n`, if itexists. Otherwise,
// it returns -1
func (n *TextNode) indexOf(target *TextNode) int {
	for i, child := range n.children {
		if child == target {
			return i
		}
	}
	return -1
}

func (n *TextNode) printPath() string {
	parents := n.findParents()
	pathParts := make([]string, len(parents)+1)
	for _, parent := range parents {
		pathParts = append(pathParts, parent.name)
	}
	pathParts = append(pathParts, n.name)
	return path.Join(pathParts...)
}

// spaces returns a string of length n containing only space characters
func spaces(n int) string {
	s := make([]byte, n)
	for i := 0; i < n; i++ {
		s[i] = ' '
	}
	return string(s)
}

func NewTextNode(name string, parent *TextNode, children []*TextNode) *TextNode {
	return &TextNode{
		parent,
		children,
		name,
	}
}
