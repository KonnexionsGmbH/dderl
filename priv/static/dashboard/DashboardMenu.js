import $ from 'jquery';
import {dderlState, alert_jq, addDashboard, ajaxCall} from '../scripts/dderl'; 

var dashboardList;

export function create(container) {
    // Set our internal reference to the container.
    dashboardList = $(container);

    var list = $('<li>');
    var buttonSave = $('<button>').addClass('heightButtons removeBorder ui-corner-flat');
    var inputToSave = $('<input type="text">').addClass('inputTextDefault');
    inputToSave.attr('id','dashboard-list-input');
    inputToSave.val("default");

    $(buttonSave).button({
        icons: { primary: "fa fa-floppy-o" },
        text : false
    });

    // Button creation
    buttonSave.click(function () {
        checkTablesNotSaved();
    });
    list.append(inputToSave);
    list.append(buttonSave);
    list.buttonset();
    container.appendChild(list[0]);
}

export function add(index, dashboard) {
    var list = $('<li>');
    var listEdit = $('<li>');
    var inputTextToEdit = $('<input type="text">').addClass('inputTextToEdit');
    inputTextToEdit.val(dashboard.getName());
    var buttonNames = $('<input type="button">').addClass('inputText');
    buttonNames.val(dashboard.getName());
    var buttonTrash = $('<button>').addClass('heightButtons removeBorder ui-corner-flat');
    var buttonEdit = $('<button>').addClass('heightButtons removeBorder ui-corner-flat');
    var buttonCancel = $('<button>').addClass('heightButtons removeBorder ui-corner-flat');
    var buttonCheck = $('<button>').addClass('heightButtons removeBorder ui-corner-flat');


    buttonTrash.button({
        icons: { primary: "fa fa-trash-o" },
        text: false
    });
    buttonEdit.button({
        icons: { primary: "fa fa-pencil" },
        text: false
    });
    buttonCheck.button({
        icons: { primary: "fa fa-check" },
        text: false
    });
    buttonCancel.button({
        icons: { primary: "fa fa-times" },
        text: false
    });

    buttonTrash.click(removeDashboard);
    buttonEdit.click(function() {
        list.hide();
        listEdit.show();
    });
    buttonCancel.click(function() {
        inputTextToEdit.val(buttonNames.val());
        list.show();
        listEdit.hide();
    });
    buttonCheck.click(function () {
        dashboard.rename(inputTextToEdit.val(), function(newName) {
            buttonNames.val(newName);
            inputTextToEdit.val(newName);
            listEdit.hide();
            list.show();
        });
    });
    buttonNames.click(function() {
        loadDashboard(dashboard);
    });

    list.append(buttonNames);
    list.append(buttonTrash, buttonEdit);
    list.buttonset();
    dashboardList.append(list);

    listEdit.append(inputTextToEdit);
    listEdit.append(buttonCancel, buttonCheck);
    listEdit.buttonset();
    dashboardList.append(listEdit);
    listEdit.hide();

    function removeDashboard() {
        var data = {id: dashboard.getId()};
        ajaxCall(null, 'delete_dashboard', data, 'delete_dashboard', function(result) {
             if(result.hasOwnProperty('error')) {
                alert_jq('<strong>remove dashboard failed!</strong><br><br>' + result.error);
            } else {
                list.remove();
                dderlState.dashboards.splice(index,1);
            }
        });
    }
}

export function save() {
    var name, dashboard, dashViews;

    name = document.getElementById("dashboard-list-input").value;
    if(name === "default") {
        alert_jq("Please select a name for the dashboard");
        return;
    }

    dashboard = findDashboard(name);
    dashViews = getCurrentViews();

    if(dashboard === null) {
        dashboard = new DDerl.Dashboard(-1, name, dashViews);
        dashboard.save(function() {
            addDashboard(dashboard);
        });
    } else {
        dashboard.updateViews(dashViews);
        dashboard.save();
    }
}

function checkTablesNotSaved() {
    var tablesNotSaved, notSavedTitles, message;
    tablesNotSaved = [];
    notSavedTitles = "";
    message = "";
    if(dderlState.currentWindows.length === dderlState.currentViews.length) {
        save();
    } else {
        notSavedTitles += "<ul>";
        for(var i = 0; i < dderlState.currentWindows.length; ++i) {
            if(!dderlState.currentWindows[i]._viewId) {
                tablesNotSaved.push(dderlState.currentWindows[i]);
                notSavedTitles += "<li>" + dderlState.currentWindows[i].options.title + "</li>";
            }
        }
        notSavedTitles += "</ul>";
        message = "The following tables are not saved as views: <br>" + notSavedTitles;
        $('<div><p><span class="ui-icon ui-icon-alert" style="float: left; margin: 0 7px 20px 0;"></span>'+ message +'</p></div>').appendTo(document.body).dialog({
            resizable: false,
            width: 450,
            height:220,
            modal: true,
            appendTo: "#main-body",
            buttons: {
                "Create views": function() {
                    $( this ).dialog( "close" );
                    dderlState.saveDashboardCounter += tablesNotSaved.length;
                    for(var i = 0; i < tablesNotSaved.length; ++i) {
                        tablesNotSaved[i].saveView();
                    }
                },
                "Ignore tables": function() {
                    $( this ).dialog( "close" );
                    save();
                },
                Cancel: function() {
                    $( this ).dialog( "close" );
                }
            },
            open: function() {
                $(this)
                    .dialog("widget")
                    .draggable("option","containment","#main-body");
            },
            close : function() {
                $(this).dialog('destroy');
                $(this).remove();
            }
        });
    }
}

function loadDashboard(dashboard) {
    $(".ui-dialog-content").dialog('close');
    $("#dashboard-list-input").val(dashboard.getName());
    dashboard.openViews();
}

function findDashboard(name) {
    for(var i = 0; i < dderlState.dashboards.length; ++i) {
        if(dderlState.dashboards[i].getName() === name) {
            return dderlState.dashboards[i];
        }
    }
    return null;
}

function getCurrentViews() {
    var resultViews, id, x, y, w, h;
    resultViews = [];
    for (var i = 0; i < dderlState.currentViews.length; ++i) {
        id = dderlState.currentViews[i]._viewId;
        x = dderlState.currentViews[i]._dlg.dialog('widget').position().left;
        y = dderlState.currentViews[i]._dlg.dialog('widget').position().top;
        w = dderlState.currentViews[i]._dlg.dialog('widget').width();
        h = dderlState.currentViews[i]._dlg.dialog('widget').height();
        resultViews.push(new DDerl.DashView(id, x, y, w, h));
    }
    return resultViews;
}