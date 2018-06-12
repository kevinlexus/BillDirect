Ext.define('TestApp.store.Store1', {
			extend: 'Ext.data.Store',
			alias  : 'store.store1',
			model: 'TestApp.model.Model1',
			controller: 'appCtrl',
			config:{
			autoLoad: true,
	                autoSync: true
			}/*,
			listeners: {
				beforeload: 'onTest'
			}*/
      });