var logins = {};
function display_login()
{
    if($('#login-button').html().indexOf('out') > 0) {
        $('#login-button').html('Log In');
        $("#db-tables-views").html('');
        $('#change-pswd-button').data("logged_in_user", "");
    }
    $('<div id="dialog-login" title="Login to DDerl" style="diaply:none">' +
      '  <table border=0 width=100% height=85% cellpadding=0 cellspacing=0>' +
      '      <tr><td align=right valign=center>User&nbsp;</td>' +
      '          <td valign=bottom><input type="text" id="user_login" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>Password&nbsp;</td>' +
      '          <td valign=bottom><input type="password" id="password_login" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '  </table>' +
      '</div>').appendTo(document.body);
    $('#dialog-login').dialog({
        autoOpen: false,
        height: 158,
        width: 300,
        resizable: false,
        modal: true,
        close: function() {
            $("#dialog-login").dialog('destroy');
            $("#dialog-login").remove();
        },
        buttons: {
            "Login": function() {
                var loginJson = {login: { user      :$('#user_login').val(),
                                          password  :MD5($('#password_login').val())}};
                ajax_post('/app/login', loginJson, null, null, function(data) {
                    if(data.login == "ok") {
                        var user = $('#user_login').val();
                        $('#change-pswd-button').data("logged_in_user", user);
                        $('#login-button').html('Log out ' + user);
                        $("#dialog-login").dialog("close");
                        display_db_login();
                    }
                    else
                        alert('Login falied : ' + data.login);
                });
            },
            Cancel: function() {
                $(this).dialog("close");
            }
        }
    });
    $('#dialog-login').dialog("open");
}

function change_password()
{
    var loggedInUser = $('#change-pswd-button').data("logged_in_user");
    if(loggedInUser == undefined || loggedInUser.length == 0) {
        alert("Please login first!");
        return;
    }

    $('<div id="dialog-change-password" title="Change DDerl account password" style="diaply:none">' +
      '  <table border=0 width=100% height=85% cellpadding=0 cellspacing=0>' +
      '      <tr><td align=right valign=center>User&nbsp;</td>' +
      '          <td valign=center><b>'+loggedInUser+'</b></td></tr>' +
      '      <tr><td align=right valign=center>Password&nbsp;</td>' +
      '          <td valign=bottom><input type="password" id="password_change_login" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>Confirm Password&nbsp;</td>' +
      '          <td valign=bottom><input type="password" id="conf_password_login" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '  </table>' +
      '</div>').appendTo(document.body);
    $('#dialog-change-password').dialog({
        autoOpen: false,
        height: 200,
        width: 300,
        resizable: false,
        modal: true,
        close: function() {
            $("#dialog-change-password").dialog('destroy');
            $("#dialog-change-password").remove();
        },
        buttons: {
            "Change Password": function() {
                if($('#conf_password_login').val() == $('#password_change_login').val()) {
                    var newPassJson = {change_pswd: { user      :loggedInUser,
                                                      password  :MD5($('#password_change_login').val())}};
                    ajax_post('/app/login_change_pswd', newPassJson, null, null, function(data) {
                        if(data.change_pswd == "ok")
                            $("#dialog-change-password").dialog("close");
                        else
                            alert('Change password falied : ' + data.change_pswd);
                    });
                }
                else alert("Confirm password missmatch!");
            },
            Cancel: function() {
                $(this).dialog("close");
            }
        }
    });
    $('#dialog-change-password').dialog("open");
}

function display_db_login()
{
    $('<div id="dialog-db-login" title="Connect to Oracle Database" style="diaply:none">' +
      '  <table border=0 width=100% height=85% cellpadding=0 cellspacing=0>' +
      '      <tr><td align=right valign=center>Connections&nbsp;</td>' +
      '          <td valign=center><select id="config_list" class="ui-corner-all"/></td></tr>' +
      '      <tr><td colspan=2><hr></td></tr>' +
      '      <tr><td align=right valign=center>Connection Name&nbsp;</td>' +
      '          <td valign=bottom><input type="text" id="name" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>IP Address&nbsp;</td>' +
      '          <td valign=bottom><input type="text" id="ip" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>DB Port&nbsp;</td>' +
      '          <td valign=bottom><input type="text" id="port" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>DB&nbsp;</td>' +
      '          <td valign=bottom><input type="text" id="service" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>DB Type&nbsp;</td>' +
      '          <td valign=center><table border=0 cellpadding=0 cellspacing=0>' +
      '              <tr><td valign=center><input type="radio" name="db_type" value="service" checked></td>' +
      '                  <td valign=center>&nbsp;Service&nbsp;&nbsp;</td>' +
      '                  <td valign=center><input type="radio" name="db_type" value="sid"></td>' +
      '                  <td valign=center>&nbsp;SID</td></tr></table>' +
      '          </td></tr>' +
      '      <tr><td align=right valign=center>Username&nbsp;</td>' +
      '          <td valign=bottom><input type="text" id="user" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '      <tr><td align=right valign=center>Password&nbsp;</td>' +
      '          <td valign=bottom><input type="password" id="password" class="text ui-widget-content ui-corner-all"/></td></tr>' +
      '  </table>' +
      '  <p class="validateTips">* All fields are mandatory</p>' +
      '</div>').appendTo(document.body);

    var name        = $("#name");
    var ip          = $("#ip");
    var port        = $("#port");
    var service     = $("#service");
    var user        = $("#user");
    var password    = $("#password");

    var allFields   = $([]).add(ip).add(port).add(service).add(user).add(password);
    var tips = $(".validateTips");

    $("#dialog-db-login").dialog({
        autoOpen: false,
        height: 400,
        width: 300,
        resizable: false,
        modal: true,
        close: function() {
            $("#dialog-db-login").dialog('destroy');
            $("#dialog-db-login").remove();
        },
        buttons: {
            "Login": function() {
                var bValid = true;
                allFields.removeClass( "ui-state-error" );

                bValid = bValid && checkLength( ip, "IP address", 7, 15 );
                bValid = bValid && checkLength( port, "port", 1, 5 );
                bValid = bValid && checkLength( service, "service", 0, 100 );
                bValid = bValid && checkLength( user, "user name", 1, 100 );
                bValid = bValid && checkLength( password, "password", 5, 16 );

                if ( bValid ) {
                    var connectJson = {connect: { ip        :ip.val(),
                                                  port      :port.val(),
                                                  service   :service.val(),
                                                  type      :$('input:radio[name=db_type]:checked').val(),
                                                  user      :user.val(),
                                                  password  :password.val()}};
                    owner = user.val();
                    ajax_post('/app/connect', connectJson, null, null, function(data) {
                        ajax_post('/app/users', null, null, null, function(data) {
                            var usr = '';
                            var userRows = data.rows;
                            for(var i = 0; i < userRows.length; ++i) {
                                    usr = userRows[i][0];
                                    $('<option value="'+usr+'" '+(usr==owner?"selected":"")+'>'+usr+'</option>').appendTo($('#users'));
                            }
                            generate_tables_views(session, owner);
                        })
                    });
                    $(this).dialog("close");
                    show_tables();
                }
            },
            "Save": function() {
                name = $("#name").val();
                saveSettings = {ip       :$("#ip").val(),
                                port     :$("#port").val(),
                                service  :$("#service").val(),
                                type     :$('input:radio[name=db_type]:checked').val(),
                                user     :$("#user").val(),
                                password :$("#password").val()};
                logins[name] = saveSettings;
                $('<option value="'+name+'">'+name+'</option>').appendTo($('#config_list'));
                load_login_form(name);

                ajax_post('/app/save', logins, null, null, function(data) {
                    alert(data.save);
                });
            },
            "Delete": function() {
                delByName = $("#name").val();
                delete logins[delByName];
                $('#config_list option[value="'+delByName+'"]').remove();
                var name = null;
                for(name in logins);
                if(null != name) load_login_form(name);

                ajax_post('/app/save', logins, null, null, function(data) {
                    alert(data.result);
                });
            },
            Cancel: function() {
                $(this).dialog("close");
            }
        }
    });
    $('#config_list').html('');
    ajax_post('/app/get_connects', null, null, null, function(data) {
        logins = data;
        for(var name in logins)
            $('<option value="'+name+'">'+name+'</option>').appendTo($('#config_list'));
        for(var name in logins) {
            load_login_form(name);
            break;
        }
    });
    $('#config_list').change(function() {
        document.title = pageTitlePrefix + $(this).val();
        load_login_form($(this).val());
    });
    
    $('#dialog-db-login').dialog("open");
}

function updateTips(t) {
    tips
        .text(t)
        .addClass("ui-state-highlight");
    setTimeout(function() {
        tips.removeClass( "ui-state-highlight", 1500 );
    }, 500 );
}

function checkLength( o, n, min, max ) {
    if ( o.val().length > max || o.val().length < min ) {
        o.addClass( "ui-state-error" );
        updateTips( "Length of " + n + " must be between " +
            min + " and " + max + "." );
        return false;
    } else {
        return true;
    }
}

function load_login_form(name) {
    $('#name').val(name);
    $('#ip').val(logins[name].ip);
    $('#port').val(logins[name].port);
    $('#service').val(logins[name].service);
    $('#sid').val(logins[name].sid);
    $('#user').val(logins[name].user);
    $('#password').val(logins[name].password);
    $('input:radio[name=db_type][value='+logins[name].type+']').click();
    $('#config_list option[value="'+name+'"]').attr("selected","selected"); 
}
