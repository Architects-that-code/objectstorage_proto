package util

// Package util provides utility functions for printing banners and spaces.

import (
	"fmt"

	"github.com/common-nighthawk/go-figure"
)

// PrintSpace prints an empty line.
func PrintSpace() {
	fmt.Println("")
}

// PrintBanner prints a colored banner with "Architects That Code" followed by an empty line.
func PrintBanner() {
	myFigure := figure.NewColorFigure("Architects That Code", "", "blue", false)
	//myFigure.Scroll(800, 100, "left")
	myFigure.Print()
	PrintSpace()
}
