/**
 * This class is the controller for the main view for the application. It is
 * specified as the "controller" of the Main view class.
 *
 * TODO - Replace this content of this view to suite the needs of your
 * application.
 */
var state="-1";
var progress="0";

Ext.define('TestApp.view.main.MainController', {
    extend: 'Ext.app.ViewController',
    requires: [
        'Ext.window.MessageBox',
	    'Ext.state.*',
        'Ext.grid.*',
        'Ext.data.*',
        'Ext.util.*',
        'Ext.form.*'
    ],

    config : {
        task: null //локальная переменная, хранящее отложенное задание
    },

    alias: 'controller.main',

    listen : { //определение собственного листенера, слушающего собственное событие контроллера: ownevent1
        controller : { //листенер и это событие сделал надуманно, чтобы попробовать технологию событий, всё можно было сделать
            '*' : {    //на обычных вызовах методов контроллера))
                ownevent1 : 'onCheck1' //вызвать onCheck1 при наступлении события
            }
        }/*,
        store: {
            '#Store1': {
                storeevent: 'onTest'
            }
        }*/
    },

    onTest: function () {
        alert('TEST2!');
    },

    //инициализация
    init: function(view) {

        //сделать отложенное задание, записать в локальную переменную task
        var runner = new Ext.util.TaskRunner(),
            task;
        me=this;
        this.setTask(runner.newTask({
            run: function() {
                me.checkState();

            },
            interval: 2000
        })
        );
        //Проверить, идёт ли формирование
        Ext.Ajax.request({
            url: 'http://127.0.0.1:8100/getStateGen',
            method: "GET",
            success: function (response) {
                state = response.responseText;
                if (state == '1') {
                    //Идет формирование, запустить отложенное задание
                    me.getTask().start();// обращение к локальной переменной контроллера task
                }
            }
        });

    },

    onCheck1: function () { // проверка
        me=this;
        //проверить наличие ошибки в последнем формировании
        Ext.Ajax.request({
            url: 'http://127.0.0.1:8100/getErrGen',
            method: "GET",
            success: function (response) {
                state = response.responseText;
                var gridErr = me.lookupReference('gridErr');
                 if (state == '1') {
                     //была ошибка, она будет отображена в основном гриде
                     gridErr.setVisible(false);
                     //спрятать грид с ошибками в лиц.счетах
                 }
                 else if (state == '2') {
                     //была ошибка, вывести грид с со списоком ошибочных лиц.счетов
                     gridErr.setVisible(true);
                 } else {
                     gridErr.setVisible(false);
                     //спрятать грид с ошибками в лиц.счетах
                 }

            }
        });

    },


    // Проверка при включении чекбокса
    checkTest: function (comp, rowIndex, checked, eOpts) {
        //обновить грид, если щелкнули на чекбоксе
            //если выбраны итоговое или переход месяца
            var grid = me.lookupReference('grid1');
            var record = grid.getStore().getAt(rowIndex);
            if (record.get('cd')=='GEN_ITG' || record.get('cd')=='GEN_MONTH_OVER'){
                me.checkItms(record.get('id'), checked);
            }
    },


    // Проверить пункты меню через базу, и поставить корректные, если надо
    checkItms: function (id, checked) {
        if (checked) {
            sel=1;
        } else {
            sel=0;
        }
        Ext.Ajax.request({
            url: 'http://127.0.0.1:8100/checkItms',
            success: function (response) {
                var grid = me.lookupReference('grid1');
                grid.getStore().load();
            },
            params :{ id : id,
                      sel: sel
                      },
            method : 'POST'
        }

        );
    },

    // Начало формирования
    onStartGen: function () {
        // вложенный запрос
        me = this;
        Ext.Ajax.request({
            url: 'http://127.0.0.1:8100/getStateGen',
            method: "GET",
            success: function (response) {
                state = response.responseText;
                var grid = me.lookupReference('grid1');
                var buttonGauge2 = me.lookupReference('buttonGauge2');
                if (state != '1') {
                    // начать формирование, если уже не идёт
                    Ext.Ajax.request({
                        url: 'http://127.0.0.1:8100/startGen'
                    });
                    me.getTask().start();// обращение к локальной переменной контроллера task
                } else {
                    console.log('Уже идет формирование!')
                }
            }
        });


    },

    // Остановка формирования
    onStopGen: function () {
        Ext.Ajax.request({
            url: 'http://127.0.0.1:8100/stopGen'
        });
    },


    ajaxFunction : function (callback) {
        ret=false;
        refreshFlag=false;
        //проверить прогресс формирования
        Ext.Ajax.request({
            url: 'http://127.0.0.1:8100/getProgress',
            method: "GET",
            success: function (response) {
                if (progress != response.responseText) {
                    refreshFlag=true;
                    progress=response.responseText;
                } else {
                    refreshFlag=false;
                }

                // вложенный запрос
                Ext.Ajax.request({
                    url: 'http://127.0.0.1:8100/getStateGen',
                    method: "GET",
                    success: function (response) {
                        state = response.responseText;
                        var grid = me.lookupReference('grid1');
                        var buttonGauge2 = me.lookupReference('buttonGauge2');
                        if (state == '1') {
                            //Идет формирование, обновить грид
                            //вызвать собственное событие контроллера
                            me.fireEvent('ownevent1', this);
                            var buttonGauge1 = me.lookupReference('buttonGauge1');
                            var i = buttonGauge1.getText();
                            if (i % 2 == 0) {
                                buttonGauge2.setIconCls('x-fa fa-file');
                            } else {
                                buttonGauge2.setIconCls('x-fa fa-file-text');
                            }
                            i++;
                            buttonGauge1.setText(i);
                            buttonGauge2.setText('Выполнение...');
                            ret = false;
                        } else if (state == '2') {
                            //Ошибка формирования
                            ret = true;
                            console.log("Ошибка");
                            //вызвать собственное событие контроллера
                            buttonGauge2.setText('Выполнить');
                            buttonGauge2.setIconCls(null);
                            me.fireEvent('ownevent1', this);
                        } else if (state == '0') {
                            //Формирование закончено успешно
                            ret = true;
                            console.log("Закончено успешно");
                            buttonGauge2.setText('Выполнить');
                            //вызвать собственное событие контроллера
                            buttonGauge2.setIconCls(null);
                            me.fireEvent('ownevent1', this);
                        }
                        if (refreshFlag) {
                            grid.getStore().reload();
                        }

                        callback(ret);
                    }
                });


            }
        });

    },

    //проверить статус, обновить грид
    checkState: function() {
        me=this;
        this.ajaxFunction(function(response) {
            //console.log("response="+response);

            if (response==true) {
                //отменить отложенное задание
                me.getTask().stop(); //отменить TaskRunner - обращение к локальной переменной контроллера task
            } else {
                //продолжение формирования
            }
        });

    },

   onSave: function () { // не используется
	var grid = this.lookupReference('grid1');
	grid.getStore().save;
	grid.getStore().sync();
        console.log("Saving...");
    },

    onConfirm: function (choice) {
        if (choice === 'yes') {
            //
        }
    }
});


