package validate

import "testing"

func TestIP4Address(t *testing.T) {
	cases := []struct {
		IP     string
		Errors int
	}{
		{
			IP:     "",
			Errors: 1,
		},
		{
			IP:     "0.0.0.0",
			Errors: 0,
		},
		{
			IP:     "1.2.3.no",
			Errors: 1,
		},
		{
			IP:     "text",
			Errors: 1,
		},
		{
			IP:     "1.2.3.4",
			Errors: 0,
		},
		{
			IP:     "12.34.43.21",
			Errors: 0,
		},
		{
			IP:     "100.123.199.0",
			Errors: 0,
		},
		{
			IP:     "255.255.255.255",
			Errors: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.IP, func(t *testing.T) {
			_, errors := IP4Address(tc.IP, "test")

			if len(errors) != tc.Errors {
				t.Fatalf("Expected IP4Address to return %d error(s) not %d", len(errors), tc.Errors)
			}
		})
	}
}

func TestMACAddress(t *testing.T) {
	cases := []struct {
		MAC    string
		Errors int
	}{
		{
			MAC:    "",
			Errors: 1,
		},
		{
			MAC:    "text d",
			Errors: 1,
		},
		{
			MAC:    "12:34:no",
			Errors: 1,
		},
		{
			MAC:    "123:34:56:78:90:ab",
			Errors: 1,
		},
		{
			MAC:    "12:34:56:78:90:NO",
			Errors: 1,
		},
		{
			MAC:    "12:34:56:78:90:ab",
			Errors: 0,
		},
		{
			MAC:    "ab:cd:ef:AB:CD:EF",
			Errors: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.MAC, func(t *testing.T) {
			_, errors := MACAddress(tc.MAC, "test")

			if len(errors) != tc.Errors {
				t.Fatalf("Expected MACAddress to return %d error(s) not %d", len(errors), tc.Errors)
			}
		})
	}
}
