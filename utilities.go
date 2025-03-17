package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func Diff[T any](a, b map[string]T) (add, remove map[string]T) {
	add = make(map[string]T)
	remove = make(map[string]T)

	for k := range a {
		_, ok := b[k]
		if !ok {
			add[k] = a[k]
		}
	}

	for k := range b {
		_, ok := a[k]
		if !ok {
			remove[k] = b[k]
		}
	}

	return add, remove
}

func Intersect[T any](a, b map[string]T) []string {
	intersection := []string{}

	if len(a) > len(b) {
		a, b = b, a
	}

	for k := range a {
		_, ok := b[k]
		if ok {
			intersection = append(intersection, k)
		}
	}

	return intersection
}

func Print(a any) {
	ajson, err := json.MarshalIndent(a, "", "  ")

	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Printf("MarshalIndent funnction output %s\n", string(ajson))
}

func ReadSchemaFile(f string) string {
	b, err := os.ReadFile(f)
	if err != nil {
		log.Fatal(err)
	}

	return string(b)
}
