/**
 * This view is an example list of people.
 */
Ext.define('TestApp.view.main.ListErr', {
    extend: 'Ext.grid.Panel',
    xtype: 'errlist',

    requires: ['TestApp.store.StoreErr',
        'Ext.selection.CellModel',
        'Ext.grid.*',
        'Ext.data.*',
        'Ext.util.*',
        'Ext.form.*'
    ],

    reference: 'gridErr',

    title: 'Ошибки!',
    iconCls: 'framing-buttons-grid',

    store: { type: 'storeErr' },

    initComponent: function() {
        Ext.apply(this, {
            //width: 800,
            height: 550
        });
        this.setVisible(false); //скрыть во время инициализации
        this.fireEvent('checkevent', this);
        this.callParent();
    },
    columns: [
        { text: 'Лиц.счет',  dataIndex: 'lsk', width: 150,
            editor: {
                readOnly:true, //только для чтения
                allowBlank: false
            }
        },
        { text: 'Описание', dataIndex: 'text',
            minWidth: 300,
            flex: 1,
            editor: {
                readOnly:true, //только для чтения
                allowBlank: false
            }
        }

    ],
    height: 300,
    frame: true
});
