package azurerm

import (
	"fmt"
	"log"
	"regexp"

	"github.com/Azure/azure-sdk-for-go/services/appinsights/mgmt/2015-05-01/insights"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/response"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmAvailabilityTest() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmAvailabilityTestCreateOrUpdate,
		Read:   resourceArmAvailabilityTestRead,
		Update: resourceArmAvailabilityTestCreateOrUpdate,
		Delete: resourceArmAvailabilityTestDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.StringMatch(
					regexp.MustCompile(`\A([a-z0-9\-\s]{1,64})\z`),
					"Name can only consist of lowercase letters, numbers, hypens, spaces and must be between 1 and 64 characters long",
				),
			},

			"location": locationSchema(),

			"resource_group_name": resourceGroupNameSchema(),

			"kind": {
				Type:             schema.TypeString,
				Optional:         true,
				Default:          string(insights.Ping),
				DiffSuppressFunc: ignoreCaseDiffSuppressFunc,
				ValidateFunc: validation.StringInSlice([]string{
					string(insights.Ping),
					string(insights.Multistep),
				}, true),
			},

			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},

			"frequency": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  300,
			},

			"timeout": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  30,
			},

			"retry_enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},

			"locations": {
				Type:     schema.TypeList,
				Required: true,
				Elem:     locationSchema(),
			},

			"configuration": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"tags": tagsSchema(),
		},
	}
}

func resourceArmAvailabilityTestCreateOrUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).availabilityTestsClient
	ctx := meta.(*ArmClient).StopContext

	log.Printf("[INFO] preparing arguments for Azure ARM Availability Test creation.")

	name := d.Get("name").(string)
	location := azureRMNormalizeLocation(d.Get("location").(string))
	resourceGroup := d.Get("resource_group_name").(string)
	kind := d.Get("kind").(string)
	description := d.Get("description").(string)
	enabled := d.Get("enabled").(bool)
	frequency := d.Get("frequency").(int)
	timeout := d.Get("timeout").(int)
	retry_enabled := d.Get("retry_enabled").(bool)
	locations := d.Get("locations").([]interface{})
	configuration := d.Get("configuration").(string)
	tags := d.Get("tags").(map[string]interface{})

	availabilityTestProperties := insights.WebTest{
		Kind:     insights.WebTestKind(kind),
		Location: &location,
		Tags:     expandTags(tags),
		WebTestProperties: &insights.WebTestProperties{
			SyntheticMonitorID: &name,
			WebTestName:        &name,
			Description:        &description,
			Enabled:            &enabled,
			Frequency:          &frequency,
			Timeout:            &timeout,
			WebTestKind:        &insights.WebTestKind(kind),
			RetryEnabled:       &retry_enabled,
			Locations:          expandLocations(locations),
			Configuration:      &insights.WebTestPropertiesConfiguration.WebTest(configuration),
		},
	}

	future, err := client.CreateOrUpdate(ctx, resourceGroup, name, availabilityTestProperties)
	if err != nil {
		return fmt.Errorf("Error issuing create request for Web Test %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	err = future.WaitForCompletion(ctx, client.Client)
	if err != nil {
		return fmt.Errorf("Error creating Web Test %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	read, err := client.Get(ctx, resourceGroup, name)
	if err != nil {
		return fmt.Errorf("Error retrieving Web Test %q (Resource Group %q): %+v", name, resourceGroup, err)
	}
	if read.ID == nil {
		return fmt.Errorf("Cannot read Web Test %s (resource group %s) ID", name, resourceGroup)
	}

	d.SetId(*read.ID)

	return resourceArmDateLakeStoreRead(d, meta)
}

func resourceArmAvailabilityTestRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).availabilityTestsClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return err
	}
	resourceGroup := id.ResourceGroup
	name := id.Path["webtests"]

	resp, err := client.Get(ctx, resourceGroup, name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[WARN] WebTest '%s' was not found (resource group '%s')", name, resourceGroup)
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error making Read request on Web Test %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	d.Set("name", name)
	d.Set("resource_group_name", resourceGroup)
	if location := resp.Location; location != nil {
		d.Set("location", azureRMNormalizeLocation(*location))
	}

	if webTestProperties := resp.WebTestProperties; webTestProperties != nil {
		d.Set("kind", string(resp.Kind))
		d.Set("description", resp.WebTestProperties.Description)
		d.Set("enabled", resp.WebTestProperties.Enabled)
		d.Set("frequency", resp.WebTestProperties.Frequency)
		d.Set("timeout", resp.WebTestProperties.Timeout)
		d.Set("retry_enabled", resp.WebTestProperties.RetryEnabled)
		d.Set("locations", flattenLocations(resp.WebTestProperties.Locations))
		d.Set("configuration", resp.WebTestProperties.WebTestPropertiesConfiguration.WebTest)
	}

	flattenAndSetTags(d, resp.Tags)

	return nil
}

func resourceArmAvailabilityTestDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).availabilityTestsClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return err
	}

	resourceGroup := id.ResourceGroup
	name := id.Path["webtests"]
	future, err := client.Delete(ctx, resourceGroup, name)
	if err != nil {
		if response.WasNotFound(future.Response()) {
			return nil
		}
		return fmt.Errorf("Error issuing delete request for Web Test %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	err = future.WaitForCompletion(ctx, client.Client)
	if err != nil {
		if response.WasNotFound(future.Response()) {
			return nil
		}
		return fmt.Errorf("Error deleting Web Test %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	return nil
}

func expandLocations(input []string) []interface{} {
	webTestLocations := make([]interface{}, 0)

	for _, location := range input {
		webTestLocation := insights.WebTestLocation(location)
		webTestLocations.append(webTestLocation)
	}

	return webTestLocations
}

func flattenLocations(input *[]insights.WebTestGeoLocation) []string {
	webTestLocations := make([]interface{}, 0)
	for _, location := range input {
		webTestLocation := string(location)
		webTestLocations.append(&webTestLocation)
	}

	return webTestLocations
}