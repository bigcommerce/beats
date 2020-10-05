// +build !integration

package udp

import (
	"testing"
)

func TestSplitGrab(t *testing.T) {

	testURL := "/stores/abcxyz123/v3/catalog/products/4018/options"

	indexParamAndExpectedValue := map[int]string{
		0: "",
		1: "stores",
		2: "abcxyz123",
		3: "v3",
		4: "catalog",
		5: "products",
		6: "4018",
		7: "options",
	}

	for k, v := range indexParamAndExpectedValue {

		val, err := splitGrab(testURL, "/", k)

		if err != nil {
			t.Errorf("Error returned: %v", err)
		}

		if val != v {
			t.Errorf("Expected value '%s' not returned, instead returned '%s'", v, val)
		}
	}

	_, err := splitGrab(testURL, "/", 8)

	if err == nil {
		t.Errorf("Should return slice index out of bounds error")
	}
}

func TestInfluxEscape(t *testing.T) {

	testsAndOutcomes := map[string]string{
		"/content/mega-menu-img/Gifts Under $150.jpg":  "/content/mega-menu-img/Gifts\\ Under\\ $150.jpg",
		"/content/mega-menu-img/Gifts Under =150.jpg":  "/content/mega-menu-img/Gifts\\ Under\\ \\=150.jpg",
		"/content/mega-menu-img/Gifts Under ,150.jpg":  "/content/mega-menu-img/Gifts\\ Under\\ \\,150.jpg",
		"/content/mega-menu-img/Gifts Under '150.jpg":  "/content/mega-menu-img/Gifts\\ Under\\ \\'150.jpg",
		"/content/mega-menu-img/Gifts Under \"150.jpg": "/content/mega-menu-img/Gifts\\ Under\\ \\\"150.jpg",
	}

	for testString, expectedOutcome := range testsAndOutcomes {

		actualOutcome := influxEscape(testString)

		if actualOutcome != expectedOutcome {
			t.Errorf("test condition '%v' failed with outcome '%v', execpted '%v'", testString, actualOutcome, expectedOutcome)
		}
	}
}
