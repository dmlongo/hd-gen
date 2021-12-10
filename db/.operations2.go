package db

func joinedAttrs(tables []Statistics) []string {
	var res []string
	var tmp map[string]bool
	for _, t := range tables {
		for _, attr := range t.attrs {
			if _, ok := tmp[attr]; !ok {
				res = append(res, attr)
				tmp[attr] = true
			}
		}
	}
	return res
}

func attrTabCount(tables []Statistics) map[string][]Statistics {
	jVars := make(map[string][]Statistics)
	for _, t := range tables {
		for _, a := range t.attrs {
			if _, ok := jVars[a]; !ok {
				jVars[a] = make([]Statistics, 0)
			}
			jVars[a] = append(jVars[a], t)
		}
	}
	return jVars
}
