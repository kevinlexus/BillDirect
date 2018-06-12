Ext.define('TestApp.model.Model1', {
    extend: 'Ext.data.Model',
            fields: ['id', 'name', 'cd', 'proc', 'sel',
                {
                    name: 'dt1',
                    dateFormat: 'Y-m-d H:i:s',
                    type: 'date'
                },
                {
                    name: 'dt2',
                    dateFormat: 'Y-m-d H:i:s',
                    type: 'date'
                }

            ],
            proxy: {
		autoSave: true,
                //autoSync: true,
                type: 'ajax',
	        api: {
                create  : '',
                read    : 'http://127.0.0.1:8100/getSprgenitm',
                update  : 'http://127.0.0.1:8100/editSprgenitm',
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