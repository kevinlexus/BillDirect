Ext.define('TestApp.controller.appCtrl', {
    extend: 'Ext.app.Controller',
    alias: 'controller.appCtrl',

    refs: [{
        ref: 'store1',
        selector: 'store'
    }],

    control: {
        store: {
            '#Store1': {
                storeevent: 'onTest'
            }
        }
    },

    /*control: {
        'store1': {
            endupdate: 'onTest'
        }
    },*/

    //refreshGrid: function() {
//        this.getList().store.load();
//    },

        /*config: {

        refs: {
            givenName: 'textfield[name="givenName"]'
        },
        control: {
            givenName: {
                elementtap: 'onGivenNameTap'
            }
        }
    },*/

/*    config: {
        control: {
            // components will have to match this component query
            // to be animated with the change view behaviour
            '#store1': {
                endupdate: 'onTest'
            }
        }
    },*/
    onTest: function () {
        alert('TEST1!');
    }
});