/**
 * This view is an example list of people.
 */
Ext.define('TestApp.view.main.List', {
    extend: 'Ext.grid.Panel',
    xtype: 'mainlist',

    requires: ['TestApp.store.Store1',
        'Ext.selection.CellModel',
        'Ext.grid.*',
        'Ext.data.*',
        'Ext.util.*',
        'Ext.form.*'
              ],

    reference: 'grid1',

    title: 'Выберите пункты для формирования',
    iconCls: 'framing-buttons-grid',

    store: { type: 'store1' },

    initComponent: function() {
        this.cellEditing = new Ext.grid.plugin.CellEditing({
            clicksToEdit: 1
        });
	Ext.apply(this, {
            //width: 100,
            height: 800,
            plugins: [this.cellEditing]
	});

	this.callParent();
    },

    columns: [
        { text: 'Наименование',  dataIndex: 'name', width: 250,
                editor: {
  		    readOnly:true, //только для чтения
                    allowBlank: false
                }
        },
	{
                xtype: 'checkcolumn',
                header: 'Выбрать?',
                dataIndex: 'sel',
                width: 90,
                stopSelection: false,
                listeners: {
                    checkchange: 'checkTest'
                    //checkchange: function (column, rowIdx, checked, eOpts) {
                    //this.fireEvent('owntest1', this);
                    //    this.up( "grid" ).fireEvent( "checkrow", comp, rowIndex, checked, eOpts  );
                        //alert('test3');
                    //change: 'onTest'
                    //}
                }
        /*,
                editor: {
                    allowBlank: false  эта штука здесь врубает какуюто странную вещь, короче нельзя сохранить значение checkbox в базе...
                }                     */
        },
	{
            text     : 'Выполнение,%',
            xtype    : 'widgetcolumn',
            width    : 120,
            dataIndex: 'proc',
            widget: {
                xtype: 'progressbarwidget',
                textTpl: [
                    '{percent:number("0")}%'
                ]
            },
                editor: {
                    readOnly:true, //только для чтения
                    allowBlank: false
                }
        },

        { text: 'Начало', dataIndex: 'dt1',
            formatter: 'date("H:i:s")',
            editor : {
                readOnly:true, //только для чтения
                allowBlank: false
            }
        }
        ,
        { text: 'Окончание', dataIndex: 'dt2',
            formatter: 'date("H:i:s")',
            editor: {
                readOnly:true, //только для чтения
                allowBlank: false
            }
        }
        ,

         { text: 'Состояние', dataIndex: 'state',
             minWidth: 300,
             flex: 1,
                 editor: {
                     readOnly:true, //только для чтения
                     allowBlank: false
                 }
         }

    ],

    dockedItems: [{
        xtype: 'toolbar',
        dock: 'bottom',
        ui: 'footer',
        layout: {
            pack: 'center'
        },
        items: [{
            minWidth: 80,
            text: 'Выполнить',
            handler: 'onStartGen'
        },{
            minWidth: 80,
            text: 'Отменить',
            handler: 'onStopGen'
        }]
    }],
    height: 300,
    frame: true,
    listeners: {
    }

});
