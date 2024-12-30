package formatter

import (
	"SequelGo/internal/ast"
	"math"
)

type CommentMapper struct {
	input string
}

type NodeCommentPair struct {
	Node    ast.Node
	Comment ast.Comment
}

type MappedComments struct {
	CommentsBefore   map[ast.Node][]ast.Comment
	CommentsSameLine map[ast.Node][]ast.Comment
	CommentsEnd      []ast.Comment
}

func mapComments(root ast.Node, comments []ast.Comment) MappedComments {
	if len(comments) == 0 {
		return MappedComments{
			CommentsBefore:   make(map[ast.Node][]ast.Comment),
			CommentsSameLine: make(map[ast.Node][]ast.Comment),
		}
	}

	mappedComments := MappedComments{
		CommentsBefore:   make(map[ast.Node][]ast.Comment),
		CommentsSameLine: make(map[ast.Node][]ast.Comment),
	}
	for _, comment := range comments {
		commentSpan := comment.Span
		shortestDistance := uint64(math.MaxUint64)
		var closestNode ast.Node

		// check if comment is on same line
		ast.Inspect(root, func(n ast.Node) bool {
			if n == nil {
				return false
			}
			nodeSpan := n.GetSpan()
			if commentSpan.StartPosition.Line != nodeSpan.StartPosition.Line ||
				commentSpan.StartPosition.Line != nodeSpan.EndPosition.Line ||
				nodeSpan.StartPosition.Line != nodeSpan.EndPosition.Line {
				return true
			}

			distance := commentSpan.StartPosition.Col - nodeSpan.StartPosition.Col
			if distance < shortestDistance {
				closestNode = n
				shortestDistance = distance
			}

			return true
		})

		// check if comment is before a node
		if shortestDistance == math.MaxUint64 {
			ast.Inspect(root, func(n ast.Node) bool {
				if n == nil {
					return false
				}
				nodeSpan := n.GetSpan()
				if commentSpan.StartPosition.Line >= nodeSpan.StartPosition.Line {
					return true
				}
				distance := nodeSpan.StartPosition.Line - commentSpan.StartPosition.Line
				if distance < shortestDistance {
					closestNode = n
					shortestDistance = distance
				}

				return true
			})
		} else {
			// found comment closest to node on same line
			nodeComments := mappedComments.CommentsSameLine[closestNode]
			nodeComments = append(nodeComments, comment)
			mappedComments.CommentsSameLine[closestNode] = nodeComments
			continue
		}

		// add to the end of the query
		if shortestDistance == math.MaxUint64 {
			mappedComments.CommentsEnd = append(mappedComments.CommentsEnd, comment)
		} else {
			// found comment closest to node before
			nodeComments := mappedComments.CommentsBefore[closestNode]
			nodeComments = append(nodeComments, comment)
			mappedComments.CommentsBefore[closestNode] = nodeComments
		}

		// if closestNode != nil {
		// 	fmt.Printf("closest node: (%s)\n", closestNode.TokenLiteral())
		// 	fmt.Printf("shortest distance: %d\n", shortestDistance)
		// } else {
		// 	fmt.Printf("closest node: (end of query)\n")
		// }
	}

	return mappedComments
}
