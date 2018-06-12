Ext.define('TestApp.store.StoreErr', {
			extend: 'Ext.data.Store',
			alias  : 'store.storeErr',
			model: 'TestApp.model.ModelErr',
			config:{
			autoLoad: true,
	                autoSync: true
			}
      });