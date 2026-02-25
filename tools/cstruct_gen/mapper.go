package main

import (
	"strings"
)

type typeKind int

const (
	kindUnknown typeKind = iota
	kindStruct
	kindPrimitive
	kindString
	kindSequence
	kindSequenceString
)

func checkStringOverride(dts map[string]string) bool {
	for _, v := range dts {
		lowerV := strings.ToLower(v)
		if strings.Contains(lowerV, "string") {
			return true
		}
	}
	return false
}

func ResolveStructName(svcMeth string, varMap map[string]string, structs map[string][]rawField, dts map[string]string) (name string, kind typeKind) {
	parts := strings.SplitN(svcMeth, ".", 2)
	if len(parts) != 2 {
		return svcMeth, kindUnknown
	}
	svc := parts[0]
	meth := parts[1]
	
	// Look up the exact variable name defined in the union: {service}_{method}
	targetVar := svc + "_" + meth
	target, ok := varMap[targetVar]
	if !ok {
		// Fallback for some request methods that may not have the Ntf_ or Evt_ prefix cleanly mapped,
		// or the variable name doesn't match exactly. But mostly they should match.
		return svcMeth, kindUnknown
	}

	// 1. Strings
	if target == "char*" || target == "std::string" || target == "String" {
		return "char*", kindString
	}

	// 2. Primitives
	switch target {
	case "uint8_t", "int8_t", "uint16_t", "int16_t", "uint32_t", "int32_t", "uint64_t", "int64_t", "float", "double", "bool":
		return target, kindPrimitive
	}

	// 3. Sequences (Array logic)
	if strings.HasPrefix(target, "dds_sequence_") || strings.Contains(target, "ARRAY_Struct") {
		if checkStringOverride(dts) {
			return target, kindSequenceString
		}
		return target, kindSequence
	}
	
	if fields, ok := structs[target]; ok {
		for _, f := range fields {
			if strings.HasPrefix(f.CType, "dds_sequence_") {
				if checkStringOverride(dts) {
					return target, kindSequenceString
				}
				return target, kindSequence
			}
		}
		return target, kindStruct
	}

	return target, kindUnknown
}
