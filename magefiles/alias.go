//go:build mage

package main

var Aliases = map[string]interface{}{
	"test": Test.Unit,
	"up":   Dev.Up,
}
