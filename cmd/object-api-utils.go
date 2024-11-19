package cmd

import "path"

const (
	slashSeparator = "/"
	SlashSeparator = "/"
	maxObjectList  = 45000
)

// pathJoin - like path.Join() but retains trailing SlashSeparator of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if HasSuffix(elem[len(elem)-1], SlashSeparator) {
			trailingSlash = SlashSeparator
		}
	}
	return path.Join(elem...) + trailingSlash
}
