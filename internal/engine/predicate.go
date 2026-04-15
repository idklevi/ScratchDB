package engine

import "fmt"

type predicate struct {
	Column   string
	Operator string
	Value    any
}

func (p *predicate) matches(row Row) (bool, error) {
	left, ok := row[p.Column]
	if !ok {
		return false, fmt.Errorf("unknown column %s", p.Column)
	}

	switch l := left.(type) {
	case int64:
		right, ok := p.Value.(int64)
		if !ok {
			return false, fmt.Errorf("cannot compare INT column %s to %T", p.Column, p.Value)
		}
		return compareInts(l, right, p.Operator)
	case string:
		right, ok := p.Value.(string)
		if !ok {
			return false, fmt.Errorf("cannot compare TEXT column %s to %T", p.Column, p.Value)
		}
		return compareStrings(l, right, p.Operator)
	default:
		return false, fmt.Errorf("unsupported column value type %T", left)
	}
}

func compareInts(left, right int64, operator string) (bool, error) {
	switch operator {
	case "=":
		return left == right, nil
	case "!=":
		return left != right, nil
	case ">":
		return left > right, nil
	case ">=":
		return left >= right, nil
	case "<":
		return left < right, nil
	case "<=":
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator %s", operator)
	}
}

func compareStrings(left, right string, operator string) (bool, error) {
	switch operator {
	case "=":
		return left == right, nil
	case "!=":
		return left != right, nil
	case ">":
		return left > right, nil
	case ">=":
		return left >= right, nil
	case "<":
		return left < right, nil
	case "<=":
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator %s", operator)
	}
}
