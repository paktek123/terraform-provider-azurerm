package azurerm

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccAzureRMDataLakeAnalytics_basic(t *testing.T) {
	resourceName := "azurerm_data_lake_analytics.test"
	ri := acctest.RandInt()
	rs := acctest.RandString(4)
	config := testAccAzureRMDataLakeAnalytics_basic(ri, rs, testLocation())

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMDataLakeAnalyticsDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMDataLakeAnalyticsExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "tier", "Consumption"),
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

func TestAccAzureRMDataLakeAnalytics_multipleADLS(t *testing.T) {
	resourceName := "azurerm_data_lake_analytics.test"
	ri := acctest.RandInt()
	rsA := acctest.RandString(4)
	rsB := acctest.RandString(4)
	config := testAccAzureRMDataLakeAnalytics_multipleADLS(ri, rsA, rsB, testLocation())

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMDataLakeAnalyticsDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMDataLakeAnalyticsExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "tier", "Consumption"),
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

func TestAccAzureRMDataLakeAnalytics_withStorageAccount(t *testing.T) {
	resourceName := "azurerm_data_lake_analytics.test"
	ri := acctest.RandInt()
	rs := acctest.RandString(4)
	config := testAccAzureRMDataLakeAnalytics_withStorageAccount(ri, rs, testLocation())

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMDataLakeAnalyticsDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMDataLakeAnalyticsExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "tier", "Consumption"),
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

func testCheckAzureRMDataLakeAnalyticsExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		// Ensure we have enough information in state to look up in API
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}

		accountName := rs.Primary.Attributes["name"]
		resourceGroup, hasResourceGroup := rs.Primary.Attributes["resource_group_name"]
		if !hasResourceGroup {
			return fmt.Errorf("Bad: no resource group found in state for data lake analytics: %s", name)
		}

		conn := testAccProvider.Meta().(*ArmClient).dataLakeAnalyticsAccountClient
		ctx := testAccProvider.Meta().(*ArmClient).StopContext

		resp, err := conn.Get(ctx, resourceGroup, accountName)
		if err != nil {
			return fmt.Errorf("Bad: Get on dataLakeAnalyticsAccountClient: %+v", err)
		}

		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("Bad: Date Lake Analytics %q (resource group: %q) does not exist", accountName, resourceGroup)
		}

		return nil
	}
}

func testCheckAzureRMDataLakeAnalyticsDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*ArmClient).dataLakeAnalyticsAccountClient
	ctx := testAccProvider.Meta().(*ArmClient).StopContext

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "azurerm_data_lake_analytics" {
			continue
		}

		accountName := rs.Primary.Attributes["name"]
		resourceGroup := rs.Primary.Attributes["resource_group_name"]

		resp, err := conn.Get(ctx, resourceGroup, accountName)
		if err != nil {
			if resp.StatusCode == http.StatusNotFound {
				return nil
			}

			return err
		}

		return fmt.Errorf("Data Lake Analytics still exists:\n%#v", resp)
	}

	return nil
}

func testAccAzureRMDataLakeAnalytics_basic(rInt int, rs string, location string) string {
	return fmt.Sprintf(`
resource "azurerm_resource_group" "test" {
  name     = "acctestRG-%d"
  location = "%s"
}

resource "azurerm_data_lake_store" "test" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
}

resource "azurerm_data_lake_analytics" "test" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
  default_data_lake_store_account_name = "${azurerm_data_lake_store.test.name}"
  data_lake_store_accounts = ["${azurerm_data_lake_store.test.name}"]
}
`, rInt, location, rs, location, rs, location)
}

func testAccAzureRMDataLakeAnalytics_multipleADLS(rInt int, rsA string, rsB string, location string) string {
	return fmt.Sprintf(`
resource "azurerm_resource_group" "test" {
  name     = "acctestRG-%d"
  location = "%s"
}

resource "azurerm_data_lake_store" "testA" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
}

resource "azurerm_data_lake_store" "testB" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
}

resource "azurerm_data_lake_analytics" "test" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
  default_data_lake_store_account_name = "${azurerm_data_lake_store.testA.name}"
  data_lake_store_accounts = ["${azurerm_data_lake_store.testA.name}", "${azurerm_data_lake_store.testB.name}"]
}
`, rInt, location, rsA, location, rsB, location, rsA, location)
}

func testAccAzureRMDataLakeAnalytics_withStorageAccount(rInt int, rs string, location string) string {
	return fmt.Sprintf(`
resource "azurerm_resource_group" "test" {
  name     = "acctestRG-%d"
  location = "%s"
}

resource "azurerm_data_lake_store" "test" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
}

resource "azurerm_storage_account" "test" {
  name                     = "unlikely23exst2acct%s"
  resource_group_name      = "${azurerm_resource_group.test.name}"
  location                 = "%s"
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_data_lake_analytics" "test" {
  name                = "unlikely23exst2acct%s"
  resource_group_name = "${azurerm_resource_group.test.name}"
  location            = "%s"
  default_data_lake_store_account_name = "${azurerm_data_lake_store.test.name}"
  data_lake_store_accounts = ["${azurerm_data_lake_store.test.name}"]
  storage_accounts = ["${azurerm_storage_account.test.name}"]
}
`, rInt, location, rs, location, rs, location, rs, location)
}
