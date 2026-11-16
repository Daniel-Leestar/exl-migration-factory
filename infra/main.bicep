// Azure storage for the lakehouse layers (Bicep, infrastructure as code).
// Azure 湖仓分层存储（Bicep，基础设施即代码）。
//
// STATUS: a template only. The team has no Azure subscription, so this file has NOT been deployed
// or validated with the Azure CLI. Review it before use.
// 状态：仅为模板。团队没有 Azure 订阅，本文件未经部署，也未用 Azure CLI 验证；使用前请先审阅。
//
// Deploy (creates one storage account with one container per layer):
//   az group create --name rg-migration-factory --location northeurope
//   az deployment group create --resource-group rg-migration-factory \
//       --template-file infra/main.bicep --parameters storageAccountName=<unique-name>

targetScope = 'resourceGroup'

@description('Globally unique storage account name: 3-24 lowercase letters and digits.')
@minLength(3)
@maxLength(24)
param storageAccountName string

@description('Azure region; defaults to the resource group location.')
param location string = resourceGroup().location

@description('One container per lakehouse layer.')
param layers array = [
  'raw'
  'bronze'
  'silver'
  'quarantine'
  'gold'
]

// Hierarchical namespace turns the account into Data Lake Storage Gen2, which Delta Lake tables need.
// 启用分层命名空间即为 Data Lake Storage Gen2，Delta Lake 表需要它。
resource account 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: account
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for layer in layers: {
  parent: blobService
  name: layer
  properties: {
    publicAccess: 'None'
  }
}]

// Pass a layer's address to the cloud scripts with --cloud --cloud-prefix.
// 通过 --cloud --cloud-prefix 把某一层的地址传给 cloud 脚本。
output goldPrefix string = 'abfss://gold@${account.name}.dfs.${environment().suffixes.storage}'
