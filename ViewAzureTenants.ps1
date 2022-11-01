####################################################
## This script can be manually run to get a full list of 
## Azure tenants in your partner portal. You can use this to
## get tenant ID's for the config files. It will create an
## excel spreadsheet named AzureTenants.xlsx and try to open it.
####################################################

If (Get-Module -ListAvailable -Name "ImportExcel") {Import-module ImportExcel} Else { install-module ImportExcel -Force; import-module ImportExcel}

Connect-MsolService
$AllCustomers = Get-MsolPartnerContract -All

if (($AllCustomers | Measure-Object).Count -gt 0) {
	$AllCustomers | Select-Object Name, DefaultDomainName, TenantID | Export-Excel .\AzureTenants.xlsx -Show
	Write-Host "$(($AllCustomers.TenantId | Measure-Object).Count) tenants found. File updated!" -ForegroundColor Green
} else {
	Write-Host "Something went wrong, no tenants were found!" -ForegroundColor Red
}

Read-Host "Press ENTER to close..." 