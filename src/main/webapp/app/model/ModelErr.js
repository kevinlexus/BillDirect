Ext.define('TestApp.model.ModelErr', {
    extend: 'Ext.data.Model',
            fields: ['id', 'lsk'],
            proxy: {
		autoSave: true,
                //autoSync: true,
                type: 'ajax',
	        api: {
                create  : '',
                read    : 'http://127.0.0.1:8100/getPrepErr',
                destroy : ''
                },
                reader: {
		   type: 'json'
                },
  	        writer: {
                   type: 'json',
		   allowSingle: false //запретить по одному отправлять отправлять объекты в Json - только массивом![объект] - иначе трудно описывать в Restful
		   /*,
                   writeAllFields: true*/  //писать весь объект в json - не имеет смысла
                }
            }
});