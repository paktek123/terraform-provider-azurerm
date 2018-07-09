package azurerm

import (
	"fmt"
	"log"
	"regexp"

	"github.com/Azure/azure-sdk-for-go/services/datalake/analytics/mgmt/2016-11-01/account"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/response"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmDataLakeAnalytics() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmDateLakeStoreCreate,
		Read:   resourceArmDateLakeStoreRead,
		Update: resourceArmDateLakeStoreUpdate,
		Delete: resourceArmDateLakeStoreDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.StringMatch(
					regexp.MustCompile(`\A([a-z0-9]{3,24})\z`),
					"Name can only consist of lowercase letters and numbers, and must be between 3 and 24 characters long",
				),
			},

			"location": locationSchema(),

			"resource_group_name": resourceGroupNameSchema(),

			"default_data_store_account_name": {
				Type:     schema.TypeString,
				Required: true,
				ValidateFunc: validation.StringMatch(
					regexp.MustCompile(`\A([a-z0-9]{3,24})\z`),
					"Name can only consist of lowercase letters and numbers, and must be between 3 and 24 characters long",
				),
			},

			"data_lake_store_accounts": {
				Type:     schema.TypeList,
				Required: false,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					ValidateFunc: validation.StringMatch(
						regexp.MustCompile(`\A([a-z0-9]{3,24})\z`),
						"Name can only consist of lowercase letters and numbers, and must be between 3 and 24 characters long",
					),
				},
			},

			"storage_accounts": {
				Type:     schema.TypeList,
				Required: false,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					ValidateFunc: validation.StringMatch(
						regexp.MustCompile(`\A([a-z0-9]{3,24})\z`),
						"Name can only consist of lowercase letters and numbers, and must be between 3 and 24 characters long",
					),
				},
			},

			"firewall_enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},

			"firewall_allow_azure_ips": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},

			"firewall_rule": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"start_ip_address": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"end_ip_address": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},

			"max_job_count": {
				Type:     schema.TypeInt,
				Optional: true,
			},

			"max_degree_of_parrallelism": {
				Type:     schema.TypeInt,
				Optional: true,
			},

			"max_degree_of_parrallelism_per_job": {
				Type:     schema.TypeInt,
				Optional: true,
			},

			"min_priority_per_job": {
				Type:     schema.TypeInt,
				Optional: true,
			},

			"query_retention": {
				Type:     schema.TypeInt,
				Optional: true,
			},

			"tier": {
				Type:             schema.TypeString,
				Optional:         true,
				Default:          string(account.Consumption),
				DiffSuppressFunc: ignoreCaseDiffSuppressFunc,
				ValidateFunc: validation.StringInSlice([]string{
					string(account.Consumption),
					string(account.Commitment500AUHours),
					string(account.Commitment5000AUHours),
					string(account.Commitment50000AUHours),
					string(account.Commitment500000AUHours),
					string(account.Commitment100AUHours),
					string(account.Commitment1000AUHours),
					string(account.Commitment10000AUHours),
					string(account.Commitment100000AUHours),
				}, true),
			},

			"tags": tagsSchema(),
		},
	}
}

func resourceArmDateLakeAnalyticsCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).dataLakeAnalyticsAccountClient
	ctx := meta.(*ArmClient).StopContext

	log.Printf("[INFO] preparing arguments for Azure ARM Date Lake Analytics creation.")

	name := d.Get("name").(string)
	location := azureRMNormalizeLocation(d.Get("location").(string))
	resource_group := d.Get("resource_group_name").(string)
	default_data_store_account_name := d.Get("default_data_store_account_name").(string)
	firewall_enabled := d.Get("firewall_enabled").(bool)
	firewall_allow_azure_ips := d.Get("firewall_allow_azure_ips").(bool)
	max_job_count := d.Get("max_job_count").(int)
	max_degree_of_parrallelism := d.Get("max_degree_of_parrallelism").(int)
	max_degree_of_parrallelism_per_job := d.Get("max_degree_of_parrallelism_per_job").(int)
	min_priority_per_job := d.Get("min_priority_per_job").(int)
	query_retention := d.Get("query_retention").(int)
	tier := d.Get("tier").(string)
	tags := d.Get("tags").(map[string]interface{})

	dateLakeAnalytics := account.CreateDataLakeAnalyticsAccountParameters{
		Location: &location,
		Tags:     expandTags(tags),
		CreateDataLakeAnalyticsAccountProperties: &account.CreateDataLakeAnalyticsAccountProperties{
			DefaultDataLakeStoreAccount: utils.String(default_data_store_account_name),
			DataLakeStoreAccounts: expandAddDataLakeStoreAccounts(d),
			StorageAccounts: expandAddStorageAccounts(d),
			FirewallRules: expandCreateFirewallRules(d),
			FirewallState: expandFirewallState(firewall_enabled),
			FirewallAllowAzureIps: expandAllowIpState(firewall_allow_azure_ips),
			NewTier: account.TierType(tier),
			MaxJobCount: utils.Int32(int32(max_job_count)),
			MaxDegreeOfParallelism: utils.Int32(int32(max_degree_of_parrallelism)),
			MaxDegreeOfParallelismPerJob: utils.Int32(int32(max_degree_of_parrallelism_per_job)),
			MinPriorityPerJob: utils.Int32(int32(min_priority_per_job)),
			QueryStoreRetention: utils.Int32(int32(query_retention)),
		},
	}

	future, err := client.Create(ctx, resource_group, name, dateLakeAnalytics)
	if err != nil {
		return fmt.Errorf("Error issuing create request for Data Lake Analytics %q (Resource Group %q): %+v", name, resource_group, err)
	}

	err = future.WaitForCompletion(ctx, client.Client)
	if err != nil {
		return fmt.Errorf("Error creating Data Lake Analytics %q (Resource Group %q): %+v", name, resource_group, err)
	}

	read, err := client.Get(ctx, resource_group, name)
	if err != nil {
		return fmt.Errorf("Error retrieving Data Lake Analytics %q (Resource Group %q): %+v", name, resource_group, err)
	}
	if read.ID == nil {
		return fmt.Errorf("Cannot read Data Lake Analytics %s (resource group %s) ID", name, resource_group)
	}

	d.SetId(*read.ID)

	return resourceArmDateLakeStoreRead(d, meta)
}

func resourceArmDateLakeAnalyticsUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).dataLakeAnalyticsAccountClient
	ctx := meta.(*ArmClient).StopContext

	name := d.Get("name").(string)
	resource_group := d.Get("resource_group_name").(string)
	firewall_enabled := d.Get("firewall_enabled").(bool)
	firewall_allow_azure_ips := d.Get("firewall_allow_azure_ips").(bool)
	max_job_count := d.Get("max_job_count").(int)
	max_degree_of_parrallelism := d.Get("max_degree_of_parrallelism").(int)
	max_degree_of_parrallelism_per_job := d.Get("max_degree_of_parrallelism_per_job").(int)
	min_priority_per_job := d.Get("min_priority_per_job").(int)
	query_retention := d.Get("query_retention").(int)
	tier := d.Get("tier").(string)
	newTags := d.Get("tags").(map[string]interface{})

	props := &account.UpdateDataLakeAnalyticsAccountParameters{
		Tags: expandTags(newTags),
		UpdateDataLakeAnalyticsAccountProperties: &account.UpdateDataLakeAnalyticsAccountProperties{
			DataLakeStoreAccounts: expandUpdateDataLakeStoreAccounts(d),
			StorageAccounts: expandUpdateStorageAccounts(d),
			FirewallRules: expandUpdateFirewallRules(d),
			FirewallState: expandFirewallState(firewall_enabled),
			FirewallAllowAzureIps: expandAllowIpState(firewall_allow_azure_ips),
			NewTier: account.TierType(tier),
			MaxJobCount: utils.Int32(int32(max_job_count)),
			MaxDegreeOfParallelism: utils.Int32(int32(max_degree_of_parrallelism)),
			MaxDegreeOfParallelismPerJob: utils.Int32(int32(max_degree_of_parrallelism_per_job)),
			MinPriorityPerJob: utils.Int32(int32(min_priority_per_job)),
			QueryStoreRetention: utils.Int32(int32(query_retention)),
		},
	}

	future, err := client.Update(ctx, resource_group, name, props)
	if err != nil {
		return fmt.Errorf("Error issuing update request for Data Lake Analytics %q (Resource Group %q): %+v", name, resource_group, err)
	}

	err = future.WaitForCompletion(ctx, client.Client)
	if err != nil {
		return fmt.Errorf("Error waiting for the update of Data Lake Analytics %q (Resource Group %q) to commplete: %+v", name, resource_group, err)
	}

	return resourceArmDateLakeStoreRead(d, meta)
}

func resourceArmDateLakeAnalyticsRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).dataLakeAnalyticsAccountClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return err
	}
	resourceGroup := id.ResourceGroup
	name := id.Path["accounts"]

	resp, err := client.Get(ctx, resourceGroup, name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[WARN] DataLakeAnalyticsAccount '%s' was not found (resource group '%s')", name, resourceGroup)
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error making Read request on Azure Data Lake Analytics %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	d.Set("name", name)
	d.Set("resource_group_name", resourceGroup)
	if location := resp.Location; location != nil {
		d.Set("location", azureRMNormalizeLocation(*location))
	}

	if datalake_analytics_account_properties := resp.DataLakeAnalyticsAccountProperties; datalake_analytics_account_properties != nil {
		d.Set("default_data_store_account_name", datalake_analytics_account_properties.DefaultDataLakeStoreAccount)
		d.Set("data_lake_store_accounts", datalake_analytics_account_properties.DataLakeStoreAccounts)
		d.Set("storage_accounts", datalake_analytics_account_properties.StorageAccounts)

		firewall_rules := flattenFirewallRules(datalake_analytics_account_properties.FirewallRules)
		if err := d.Set("firewall_rules", firewall_rules); err != nil {
			return fmt.Errorf("Error flattening `firewall_rules`: %s", err)
		}

		d.Set("firewall_enabled", getFirewallState(datalake_analytics_account_properties.FirewallState))
		d.Set("firewall_allow_azure_ips", getAllowIpState(datalake_analytics_account_properties.FirewallAllowAzureIps))
		d.Set("max_job_count", datalake_analytics_account_properties.MaxJobCount)
		d.Set("max_degree_of_parrallelism", datalake_analytics_account_properties.MaxDegreeOfParallelism)
		d.Set("max_degree_of_parrallelism_per_job", datalake_analytics_account_properties.MaxDegreeOfParallelismPerJob)
		d.Set("min_priority_per_job", datalake_analytics_account_properties.MinPriorityPerJob)
		d.Set("query_retention", datalake_analytics_account_properties.QueryStoreRetention)
		d.Set("tier", string(datalake_analytics_account_properties.CurrentTier))
	}

	flattenAndSetTags(d, resp.Tags)

	return nil
}

func resourceArmDateLakeAnalyticsDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).dataLakeAnalyticsAccountClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return err
	}

	resourceGroup := id.ResourceGroup
	name := id.Path["accounts"]
	future, err := client.Delete(ctx, resourceGroup, name)
	if err != nil {
		if response.WasNotFound(future.Response()) {
			return nil
		}
		return fmt.Errorf("Error issuing delete request for Data Lake Analytics %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	err = future.WaitForCompletion(ctx, client.Client)
	if err != nil {
		if response.WasNotFound(future.Response()) {
			return nil
		}
		return fmt.Errorf("Error deleting Data Lake Analytics %q (Resource Group %q): %+v", name, resourceGroup, err)
	}

	return nil
}

func expandAddDataLakeStoreAccounts(d *schema.ResourceData) *[]account.AddDataLakeStoreWithAccountParameters {
	dataLakeStoreAccounts := d.Get("data_lake_store_accounts").([]string)
	addDataLakeStoreWithAccountParameters := make([]account.AddDataLakeStoreWithAccountParameters, 0)

	for _, name := range dataLakeStoreAccounts {
		addDataLakeStoreWithAccountParameters = append(addDataLakeStoreWithAccountParameters, account.AddDataLakeStoreWithAccountParameters{
			Name: &name,
		})
	}

	return &addDataLakeStoreWithAccountParameters
}

func expandUpdateDataLakeStoreAccounts(d *schema.ResourceData) *[]account.UpdateDataLakeStoreWithAccountParameters {
	dataLakeStoreAccounts := d.Get("data_lake_store_accounts").([]string)
	updateDataLakeStoreWithAccountParameters := make([]account.UpdateDataLakeStoreWithAccountParameters, 0)

	for _, name := range dataLakeStoreAccounts {
		updateDataLakeStoreWithAccountParameters = append(updateDataLakeStoreWithAccountParameters, account.UpdateDataLakeStoreWithAccountParameters{
			Name: &name,
		})
	}

	return &updateDataLakeStoreWithAccountParameters
}

func flattenDataLakeStoreAccounts(input *[]account.AddDataLakeStoreWithAccountParameters) interface{} {
	results := make([]string, 0)

	if input != nil {
		for _, v := range *input {
			results = append(results, *v.Name)
		}
	}

	return results
}

func expandAddStorageAccounts(d *schema.ResourceData) *[]account.AddStorageAccountWithAccountParameters {
	storageAccounts := d.Get("storage_accounts").([]string)
	addStorageAccountWithAccountParameters := make([]account.AddStorageAccountWithAccountParameters, 0)

	for _, name := range storageAccounts {
		addStorageAccountWithAccountParameters = append(addStorageAccountWithAccountParameters, account.AddStorageAccountWithAccountParameters{
			Name: &name,
		})
	}

	return &addStorageAccountWithAccountParameters
}

func expandUpdateStorageAccounts(d *schema.ResourceData) *[]account.UpdateStorageAccountWithAccountParameters {
	storageAccounts := d.Get("storage_accounts").([]string)
	updateStorageAccountWithAccountParameters := make([]account.UpdateStorageAccountWithAccountParameters, 0)

	for _, name := range storageAccounts {
		updateStorageAccountWithAccountParameters = append(updateStorageAccountWithAccountParameters, account.UpdateStorageAccountWithAccountParameters{
			Name: &name,
		})
	}

	return &updateStorageAccountWithAccountParameters
}

func flattenStorageAccounts(input *[]account.AddStorageAccountWithAccountParameters) interface{} {
	results := make([]string, 0)

	if input != nil {
		for _, v := range *input {
			results = append(results, *v.Name)
		}
	}

	return results
}

func expandCreateFirewallRules(d *schema.ResourceData) *[]account.CreateFirewallRuleWithAccountParameters {
	firewallRules := d.Get("firewall_rule").([]interface{})
	createFirewallRuleWithAccountParameters := make([]account.CreateFirewallRuleWithAccountParameters, 0)

	for _, rule := range firewallRules {
		firewall_rule := rule.(map[string]interface{})

		name := firewall_rule["name"].(string)
		start_ip_address := firewall_rule["start_ip_address"].(string)
		end_ip_address := firewall_rule["end_ip_address"].(string)

		createFirewallRuleWithAccountParameters = append(createFirewallRuleWithAccountParameters, account.CreateFirewallRuleWithAccountParameters{
			Name: &name,
			CreateOrUpdateFirewallRuleProperties: &account.CreateOrUpdateFirewallRuleProperties{
				StartIPAddress: &start_ip_address,
				EndIPAddress: &end_ip_address,
			},
		})
	}

	return &createFirewallRuleWithAccountParameters
}

func expandUpdateFirewallRules(d *schema.ResourceData) *[]account.UpdateFirewallRuleWithAccountParameters {
	firewallRules := d.Get("firewall_rule").([]interface{})
	updateFirewallRuleWithAccountParameters := make([]account.UpdateFirewallRuleWithAccountParameters, 0)

	for _, rule := range firewallRules {
		firewall_rule := rule.(map[string]interface{})

		name := firewall_rule["name"].(string)
		start_ip_address := firewall_rule["start_ip_address"].(string)
		end_ip_address := firewall_rule["end_ip_address"].(string)

		updateFirewallRuleWithAccountParameters = append(updateFirewallRuleWithAccountParameters, account.UpdateFirewallRuleWithAccountParameters{
			Name: &name,
			UpdateFirewallRuleProperties: &account.UpdateFirewallRuleProperties{
				StartIPAddress: &start_ip_address,
				EndIPAddress: &end_ip_address,
			},
		})
	}

	return &updateFirewallRuleWithAccountParameters
}

func flattenFirewallRules(input *[]account.FirewallRule) interface{} {
	results := make([]interface{}, 0)

	if input != nil {
		for _, v := range *input {
			result := make(map[string]interface{}, 0)
			result["name"] = v.Name
			result["start_ip_address"] = *v.StartIPAddress
			result["end_ip_address"] = *v.EndIPAddress
			results = append(results, result)
		}
	}

	return results
}

func expandFirewallState(input bool) account.FirewallState {
	if input == true {
		return account.FirewallStateEnabled
	}

	return account.FirewallStateDisabled
}

func expandAllowIpState(input bool) account.FirewallAllowAzureIpsState {
	if input == true {
		return account.Enabled
	}

	return account.Disabled
}

func getFirewallState(input account.FirewallState) bool {
	if input == account.FirewallStateEnabled {
		return true
	} 

	return false
} 

func getAllowIpState(input account.FirewallAllowAzureIpsState) bool {
	if input == account.Enabled {
		return true
	} 

	return false
} 
