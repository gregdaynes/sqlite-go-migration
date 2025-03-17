package main

type Table struct {
	Name    string
	SQL     string
	Columns map[string]TableColumn
}

type TableColumn struct {
	Name         string
	Type         string
	NotNull      bool
	DefaultValue any
	PrimaryKey   bool
}

type TableColumns map[string]TableColumn
