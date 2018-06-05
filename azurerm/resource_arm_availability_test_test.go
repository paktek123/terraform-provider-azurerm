package azurerm

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccAzureRMAvailabilityTest_basic(t *testing.T) {
	resourceName := "azurerm_availability_test.test"
	ri := acctest.RandInt()
	config := testAccAzureRMAvailabilityTest_basic(ri, testLocation())

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMAvailabilityTestDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMAvailabilityTestExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "kind", "ping"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testCheckAzureRMAvailabilityTestExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		// Ensure we have enough information in state to look up in API
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}

		testName := rs.Primary.Attributes["name"]
		resourceGroup, hasResourceGroup := rs.Primary.Attributes["resource_group_name"]
		if !hasResourceGroup {
			return fmt.Errorf("Bad: no resource group found in state for web test: %s", name)
		}

		conn := testAccProvider.Meta().(*ArmClient).availabilityTestsClient
		ctx := testAccProvider.Meta().(*ArmClient).StopContext

		resp, err := conn.Get(ctx, resourceGroup, testName)
		if err != nil {
			return fmt.Errorf("Bad: Get on AvailabilityClient: %+v", err)
		}

		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("Bad: AvailabilityTest %q (resource group: %q) does not exist", testName, resourceGroup)
		}

		return nil
	}
}

func testCheckAzureRMAvailabilityTestDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*ArmClient).availabilityTestsClient
	ctx := testAccProvider.Meta().(*ArmClient).StopContext

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "azurerm_availability_test" {
			continue
		}

		testName := rs.Primary.Attributes["name"]
		resourceGroup := rs.Primary.Attributes["resource_group_name"]

		resp, err := conn.Get(ctx, resourceGroup, testName)
		if err != nil {
			if resp.StatusCode == http.StatusNotFound {
				return nil
			}

			return err
		}

		return fmt.Errorf("Availibility Test still exists:\n%#v", resp)
	}

	return nil
}

func testAccAzureRMAvailabilityTest_basic(rInt int, location string) string {
	return fmt.Sprintf(`
resource "azurerm_resource_group" "test" {
  name     = "acctestRG_%d"
  location = "%s"
}

resource "azurerm_availability_test" "test" {
  name                = "webtest%d"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
  kind				  = "ping"
  locations           = ["%s", "northeurope"]
}
`, rInt, location, rInt, location, location)
}
