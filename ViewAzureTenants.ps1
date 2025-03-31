####################################################
## This script can be manually run to get a full list of 
## Azure tenants in your partner portal. You can use this to
## get tenant ID's for the config files. It will create an
## excel spreadsheet named AzureTenants.xlsx and try to open it.
####################################################

If (Get-Module -ListAvailable -Name "ImportExcel") {Import-module ImportExcel} Else { install-module ImportExcel -Force; import-module ImportExcel}
If (Get-Module -ListAvailable -Name "Microsoft.Graph.Identity.DirectoryManagement") {Import-module Microsoft.Graph.Identity.DirectoryManagement} Else { install-moduleMicrosoft.Graph.Identity.DirectoryManagement -Force; import-module Microsoft.Graph.Identity.DirectoryManagement}

. "$PSScriptRoot\Config Files\APIKeys.ps1" # API Keys

$AzureConnected = $false
if ($AzureAppCredentials_AllTenants -and $Azure_TenantID) {
	$AuthBody = @{
		grant_type		= "client_credentials"
		scope			= "https://graph.microsoft.com/.default"
		client_id		= $AzureAppCredentials_AllTenants.AppID
		client_secret	= $AzureAppCredentials_AllTenants.ClientSecret
	}

	$conn = Invoke-RestMethod `
		-Uri "https://login.microsoftonline.com/$($AzureAppCredentials_AllTenants.TenantID)/oauth2/v2.0/token" `
		-Method POST `
		-Body $AuthBody

	$AzureToken = ConvertTo-SecureString -String $conn.access_token -AsPlainText -Force
	$MgGraphConnect = Connect-MgGraph -AccessToken $AzureToken
	if ($MgGraphConnect -like "Welcome To Microsoft Graph!*") {
		$AzureConnected = $true
	}
}

if (!$AzureConnected) {
	Write-Error "There was an error connecting to Azure. Exiting..."
	exit
}

$AllCustomers = Get-MgContract -All

if (($AllCustomers | Measure-Object).Count -gt 0) {
	$AllCustomers | Select-Object DisplayName, DefaultDomainName, @{N="TenantId";E={$_.CustomerId}} | Export-Excel .\AzureTenants.xlsx -Show
	Write-Host "$(($AllCustomers.CustomerId | Measure-Object).Count) tenants found. File updated!" -ForegroundColor Green
} else {
	Write-Host "Something went wrong, no tenants were found!" -ForegroundColor Red
}

Read-Host "Press ENTER to close..." 