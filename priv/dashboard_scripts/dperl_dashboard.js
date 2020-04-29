function init(container, width, height) {
    "use strict";
    // This code is executed once and it should initialize the graph.

// Dashboard main view, should be named: "cpro_dashboard":
/*

select
    ckey
    ,
    cvalue
    ,
    chash
  from
    CPROS
  where
        safe_string
          (
            hd(ckey) 
          )
      <>
        to_string('register') 
    and
        safe_string
          (
            nth(4,ckey) 
          )
      <>
        to_string('focus') 
*/


// Focus sql view, should be named "cpro_dashboard_focus":
/*

select
    ckey
    ,
    cvalue
    ,
    chash
  from
    CPROS
  where
          safe_string
            (
            hd(ckey) 
            ) 
        <>
          to_string('register') 
      and
          safe_string
            (
            nth(4,ckey) 
            ) 
        <>
          to_string('focus') 
    or
        safe_list
          (
            nth(6,ckey) 
          )
      =
        list
          (
            to_string(:binstr_focus_name) 
            ,
            to_string(:binstr_focus_key) 
          )
*/

// Error drill down view, should be named "cpro_dashboard_drill_error":
/*

select
    ckey
    ,
    cvalue
    ,
    chash
  from
    cpro.cproJobError
  where
      safe_string
        (
            hd(ckey) 
        )
    =
      to_string(:binstr_jobname)
*/

// Channel drill down view, should be named "cpro_dashboard_drill_channel":
/*

select
    ckey
    ,
    cvalue
    ,
    chash
  from
  :binstr_channel

*/

    /** Size & positioning parameters */
    
    // virtual coordinates drawing arc radius
    var vArcRadius = 1000;
    // node radius in virtual coordinates
    var nradius = 100;
    var animDuration = 500;

    // Focusname or topic for subscriptions
    var topic = "shortid";
    var viewName = "cpro_dashboard";
    var focusSuffix = "_focus";

    var initGraph = function() {
        return {
            links: {},
            nodes: {},
            status: {},
            errors: {},
            channelIdMap: {},
            focusList: {},
            jobIdChannelMap: {},
            center: {
                // Position relative to the bottom center after margin.
                CPROS: { 
                    position: { x: -1.9 * nradius, y: 0.3 * nradius },
                    status: 'idle',
                    system_information: {}
                },
                CPROP: {
                    position: { x: 0, y: -nradius },
                    status: 'idle',
                    system_information: {}
                }
            }
        };
    }

    /** Helper functions for data extraction */
    var parseError = function(term) {
        return new Error(term + " is not a valid json term");
    }
    var getKey = function(row) {
        var k = [];
        try {
            k = JSON.parse(row.ckey_1);
        } catch (e) {
            throw parseError(row.ckey_1);
        }
        return k;
    };

    var getValue = function(row) {
        var v = {};
        try {
            v = JSON.parse(row.cvalue_2);
        } catch (e) {
            throw parseError(row.cvalue_2);
        }
        return v;
    };

    function isNodeRunning(nodeId, links) {
        var jobs;
        for(var lid in links) {
            if(links[lid].target === nodeId) {
                jobs = links[lid].jobs;
                for(var jId in jobs) {
                    if(jobs[jId].running === true) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    var extractLinksNodes = function(rows, graph) {
        var links = graph.links;
        var nodes = graph.nodes;
        var status = graph.status;
        var center = graph.center;
        var errors = graph.errors;
        var channelIdMap  = graph.channelIdMap;
        var focusList = graph.focusList;
        var jobIdChannelMap = graph.jobIdChannelMap;

        rows.forEach(function(row) {
            var key = getKey(row);
            var value = getValue(row);

            // All keys we are interested should have 7 elements.
            if(key.length !== 7) {
                // Discard any other rows.
                return;
            }

            if(key[5] === "job_status") {
                var triangleId = key[2] + '_' + key[3] + '_' + key[4];
                var jobId = key[2] + '_' + key[3];
                if (row.op === "del") {
                    delete status[triangleId];
                } else { 
                    status[triangleId] = {
                        id: triangleId,
                        job: jobId,
                        status: value.status
                    };
                }
            } else if(key[5] == "error" && key[3] == "system_info") {
                if (row.op === "del") {
                    delete center[key[2]][key[6] + "_error"];
                    var still_errors = false;
                    var centerOtherKeys = Object.keys(center[key[2]]);
                    for(var kIdx = 0; kIdx < centerOtherKeys.length; ++kIdx) {
                        if(centerOtherKeys[kIdx].includes("_error")) {
                            still_errors = true;
                            break;
                        }
                    }
                    if (!still_errors) {
                        center[key[2]].status = "idle";
                    }
                } else {
                    console.log("Setting status error");
                    center[key[2]].status = "error";
                    center[key[2]][key[6] + "_error"] = value;
                }
            } else if(key[5] === "jobs") {
                var nodeId = value.platform;
                var desc = value.desc || "";
                if(value.direction !== "pull") {
                    channelIdMap[value.channel] = nodeId;
                }
                if(!center.hasOwnProperty(nodeId)) {
                    if(!nodes.hasOwnProperty('nodeId')) {
                        nodes[nodeId] = {
                            id: nodeId,
                            status: "stopped",
                            desc: desc
                        };
                    } else {
                        nodes[nodeId].desc = desc;
                    }
                }
                var linkId = key[2] + '_' + nodeId;
                var jobId = key[2] + '_' + key[3];
                jobIdChannelMap[jobId] = value.channel;
                var jobs = {};
                if(links.hasOwnProperty(linkId)) {
                    jobs = links[linkId].jobs;
                }
                jobs[jobId] = {
                    id: jobId,
                    legend: key[3],
                    enabled: value.enabled,
                    running: value.running,
                    direction: value.direction
                };
                var runningLink = false;
                for(var jId in jobs) {
                    if(jobs[jId].running === true) {
                        runningLink = true;
                        break;
                    }
                }
                links[linkId] = {
                    id: linkId,
                    source: key[2],
                    target: nodeId,
                    running: runningLink,
                    jobs: jobs
                };
                if(isNodeRunning(nodeId, links) && nodes[nodeId].status === "stopped") {
                    nodes[nodeId].status = "idle";
                }
            } else if(key[3] === "focus") {
                var channel = key[6][0];
                var focusValueKey = key[6][1];
                if (typeof key[6][1].join === 'function') {
                    focusValueKey = key[6][1].join("_");
                }
                if(!focusList.hasOwnProperty(channel)) {
                    focusList[channel] = {};
                }
                focusList[channel][focusValueKey] = value;
            } else if(key[5] === "error") {
                var jobId = key[2] + '_' + key[3];
                if (row.op === "del" && errors.hasOwnProperty(jobId)) {
                    if(key[3] === key[6]) {
                        errors[jobId].details = "";
                    } else {
                        var idx = errors[jobId].ids.indexOf(key[6].toString());
                        if(idx > -1) {
                            errors[jobId].ids.splice(idx, 1);
                        }
                    }
                    if(errors[jobId].ids.length === 0 && !errors[jobId].details) {
                        delete errors[jobId];
                    }
                } else {
                    if(!errors.hasOwnProperty(jobId)) {
                        errors[jobId] = {
                            details: "",
                            ids: []
                        };
                    }
                    if(key[3] === key[6]) {
                        errors[jobId].details = value;
                    } else {
                        if(errors[jobId].ids.indexOf(key[6].toString()) === -1) {
                            errors[jobId].ids.push(key[6].toString());
                        }
                    }
                }
            } else if(key[5] === "system_info") {
                var centerId = key[2];
                var systemIp = key[4];
                if(center[centerId]) {
                    if(key[6] === "node_status" && center[centerId].system_information) {
                        var systemInfo = {};
                        for(var k in value) {
                            systemInfo[k] = value[k];
                        }
                        center[centerId].system_information[systemIp] = systemInfo;
                    } else if(key[6] === "heartbeat") {
                        var collectorName = key[0];
                        if(row.op !== "del" && (value.cpu_overload_count !== 0 || value.memory_overload_count !== 0 || value.eval_crash_count !== 0)) {
                            center[centerId].status = "error";
                            var ovErr = {};
                            if(!center[centerId].overload_error) {
                                ovErr[collectorName] = {};
                                ovErr[collectorName][systemIp] = value;
                                center[centerId].overload_error = ovErr;
                            } else if (!center[centerId].overload_error[collectorName]) {
                                ovErr[systemIp] = value;
                                center[centerId].overload_error[collectorName] = ovErr;
                            } else {
                                center[centerId].overload_error[collectorName][systemIp] = value;
                            }
                        } else {
                            // TODO: Is there a simple way of doing this ? 
                            if(center[centerId].overload_error && center[centerId].overload_error[collectorName] && center[centerId].overload_error[collectorName][systemIp]) {
                                delete center[centerId].overload_error[collectorName][systemIp];
                                if(Object.keys(center[centerId].overload_error[collectorName]).length === 0) {
                                    delete center[centerId].overload_error[collectorName];
                                    if(Object.keys(center[centerId].overload_error).length === 0) {
                                        delete center[centerId].overload_error;
                                        var still_errors = false;
                                        var centerOtherKeys = Object.keys(center[centerId]);
                                        for(var kIdx = 0; kIdx < centerOtherKeys.length; ++kIdx) {
                                            if(centerOtherKeys[kIdx].includes("_error")) {
                                                still_errors = true;
                                                break;
                                            }
                                        }
                                        if (!still_errors) {
                                            center[centerId].status = "idle";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Reset the status as errors could have been deleted
        for(var nid in nodes) {
            if(nodes[nid].status === "error" || (isFocus && nodes[nid].running_status !== undefined)) {
                nodes[nid].status = nodes[nid].running_status;
            }
        }

        console.log("the errors", errors);
        for(var lid in links) {
            var linknode = links[lid].target;
            if(nodes.hasOwnProperty(linknode)) {
                for(var jId in links[lid].jobs) {
                    if(errors.hasOwnProperty(jId)) {
                        nodes[linknode][jId + "_error"] = errors[jId];
                        if(nodes[linknode].status !== "error") {
                            nodes[linknode].running_status = nodes[linknode].status;
                            nodes[linknode].status = "error";
                        }
                    } else {
                        delete nodes[linknode][jId + "_error"];
                    }
                }
            } else {
                console.log("link without node found", linknode);
            }
        }

        // Grey out all the platforms before appending focus data
        if(isFocus) {
            for(nid in nodes) {
                if(nodes[nid].status !== "error") {
                    nodes[nid].running_status = nodes[nid].status;
                    nodes[nid].status = "stopped";
                }
            }
        }

        // Appending focus to the correct platform
        console.log("The focus list", focusList);
        console.log("the channel id map", channelIdMap);
        for(var channel in channelIdMap) {
            if(focusList.hasOwnProperty(channel)) {
                var nid = channelIdMap[channel];
                if(!nodes[nid].status !== "error") {
                    nodes[nid].status = nodes[nid].running_status;
                }
                for(var focusValueKey in focusList[channel]) {
                    nodes[nid][focusValueKey] = focusList[channel][focusValueKey];
                }
            } 
        }

        for(var sid in status) {
            status[sid].channel = jobIdChannelMap[status[sid].job];
        }

        return {links: links, nodes: nodes, status: status, center: center, errors: errors, channelIdMap: channelIdMap, focusList: focusList, jobIdChannelMap: jobIdChannelMap};
    };
    /** End data extraction functions */

    // Add the focus input 
    var focusDiv = container
        .append('div')
        .style('position', 'absolute')
        .style("margin", "10px 0px 0px 10px");

    var inputId = "shortid_" + Math.random().toString(36).substr(2, 14);

    var label = focusDiv
        .append('label')
        .attr("for", inputId)
        .text("Shortid: ");

    var focusInput = focusDiv
        .append('input')
        .attr("id", inputId)
        .style("border", "solid 1px black")
        .style("border-radius", "3px")
        .style("padding", "1px 0px 1px 4px");

    var graph = initGraph();
    var firstData = true;
    var isFocus = false;

    focusInput.on("keypress", function() {
        if(d3.event.keyCode == 13) {
            var inp = focusInput.node();
            console.log("the value", inp.value);
            // TODO: Do not call if it is the same registration again ? or maybe for refresh ?
            var params = {
                ':binstr_focus_name' : {typ: "binstr", val: topic},
                ':binstr_focus_key'  : {typ: "binstr", val: inp.value}
            }
            helper.req(viewName, focusSuffix, topic, inp.value, params, function() {
                svg.selectAll('svg > *').remove();
                graph = initGraph();
                firstData = true;
                if (inp.value) {
                    isFocus = true;
                } else {
                    isFocus = false;
                }
            });
        }
    })

    var margin = { top: 10, right: 10, bottom: 10, left: 10 }; // physical margins in px

    var colorStatus = {
        idle: 'green',
        error: 'red',
        synced: 'yellow',
        cleaning: 'lightsteelblue',
        cleaned: 'blue',
        refreshing: 'cornflowerblue',
        refreshed: 'purple',
        stopped: 'lightgrey'
    };
    // To see the complete circle when drawing negative coordinates
    // and width and height for the virtual coordinates
    var vBox = {
        x: -1 * (vArcRadius + nradius),
        y: -1 * (vArcRadius + nradius),
        w: vArcRadius * 2 + nradius * 2,
        h: vArcRadius + 3 * nradius
    };

    var svg = container
        .append('svg')
        .attr('viewBox', vBox.x + ' ' + vBox.y + ' ' + vBox.w + ' ' + vBox.h)
        .attr('preserveAspectRatio', 'xMidYMax meet')
        .style('margin-top', margin.top + 'px')
        .style('margin-right', margin.right + 'px')
        .style('margin-bottom', margin.bottom + 'px')
        .style('margin-left', margin.left + 'px');

    function resize(w, h) {
        var cheight = h - (margin.top + margin.bottom);
        var cwidth = w - (margin.left + margin.right);
        svg.attr('width', cwidth)
            .attr('height', cheight);
    }

    resize(width, height);

    var tooltipDiv = d3.select("body").append('div')
        .styles({
            position: "absolute",
            "text-align": "left",
            padding: "2px",
            font: "12px courier",
            border: "0px",
            "border-radius": "8px",
            "pointer-events": "none",
            opacity: 0,
            "z-index": 99996,
            "background-color": "lightgrey",
            "max-width": "calc(100% - 10px)",
            "max-height": "calc(100% - 40px)",
            overflow: "hidden"
        });

    function tooltipStringifyFilter(name, val) {
        if(name === "position"){
            return undefined;
        }
        return val;
    }

    function showTooltip(d) {
        var html = formatJSON(JSON.stringify(d, tooltipStringifyFilter, 2), true);
        apply_transition(tooltipDiv.html(html), 200).style('opacity', 0.95);
    }

    function formatJSON(json, preformatted) {
        json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        var result = json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            var color = 'brown'; // number
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    match = match.slice(1, -2) + ":";
                    color = 'blue'; // key
                } else {
                    color = 'green'; // string
                }
            } else if (/true|false/.test(match)) {
                color = 'magenta'; // boolean
            } else if (/null/.test(match)) {
                color = 'red'; //null
            }
            return '<span style="color:' + color + '">' + match + '</span>';
        });
        if(preformatted) {
            result = "<pre>" + result + "</pre>";
        }
        return result;
    }

    function moveTooltip() {
        // Position the tooltip without letting it go outside the window.
        var availableHeight = document.documentElement.clientHeight;
        var availableWidth = document.documentElement.clientWidth;

        var d = tooltipDiv.node();
        var tooltipHeight = d.scrollHeight;
        var tooltipWidth = d.scrollWidth;

        var left = d3.event.pageX + 15;
        if(left + tooltipWidth + 5 > availableWidth) {
            left = Math.max(availableWidth-tooltipWidth-5, 5);
        }
        var top = d3.event.pageY + 15;
        if(top + tooltipHeight + 5 > availableHeight) {
            top = Math.max(availableHeight-tooltipHeight-5, 30);
        }

        tooltipDiv
            .style('left', left + "px")
            .style('top', top + "px");
    }

    function hideTooltip() {
        apply_transition(tooltipDiv, animDuration).style('opacity', 0);
    }

    function openErrorView(d) {
        var found = false;
        var drillKey = "";
        var sp;
        for(var k in d) {
            sp = k.split("_");
            if(sp[2] === "error") {
                drillKey = sp[1];
                found = true;
                break;
            }
        }
        if(found) {
            helper.browse('cpro_dashboard_drill_error', {
                ':binstr_jobname' : {typ: "binstr", val: drillKey}
            });
        }
    }

    function openChannelView(d) {
        if(d.channel) {
            helper.browse('cpro_dashboard_drill_channel', {
                ':binstr_channel': {typ: "binstr", val: d.channel}
            });
        }
    }

    function entries(obj) {
        var res = [];
        for(var k in obj) {
            obj[k].id = k;
            res.push(obj[k]);
        }
        return res;
    }

    function splitText(text) {
        var text = text.split(":").pop();
        return text.split(/\s+/);
        /* Disabled until we have grouping
        if(text.length < 10) {
            return [text];
        } else {
            
            var words = text.split(/\s+/);
            var lines = [words[0]];
            var maxLengthLine = 8;
            // TODO: try to do this dynamic, lines on the top are smaller than lines on the bottom, how can we divide the lines to represent that ?
            for(var i = 1, j = 0; i < words.length; ++i) {
                if(lines[j].length + words[i].length <= maxLengthLine) {
                    lines[j] += ' ' + words[i];
                } else {
                    j += 1;
                    lines[j] = words[i];
                }
            }
            return lines;
        }
        */
    }

    function apply_transition(d3Obj, duration) {
        if (!document.hidden) {
            return d3Obj.transition().duration(duration);
        }
        return d3Obj;
    }

    function contextMenu(d) {
        d3.event.preventDefault();
        var menuSpec = [
            {
                label: "Show details",
                icon: "external-link",
                cb: function(evt) {
                    var pos = {x: evt.pageX - 25, y: evt.pageY - 50}
                    var html = formatJSON(JSON.stringify(d, tooltipStringifyFilter, 2), true);
                    helper.openDialog(splitText(d.id).join(" "), html, pos);
                }
            }
        ];
        var pos = {x: d3.event.pageX - 15, y: d3.event.pageY - 20};
        helper.contextMenu(menuSpec, pos);
    }

    return {
        on_data: function(data) {
            if(data.length === 0) {
                return;
            }

            if(firstData) {
                firstData = false;

                // Add center nodes
                var NewCenterNodesDom = svg.selectAll('.center-nodes')
                    .data(entries(graph.center), function(d) {
                        return d.id;
                    })
                    .enter()
                    .append('g')
                    .attr("class", "center-nodes")
                    .on('mouseover', showTooltip)
                    .on('mousemove', moveTooltip)
                    .on('mouseout', hideTooltip)
                    .on("contextmenu", contextMenu);

                NewCenterNodesDom.append('circle')
                    .attr('r', nradius)
                    .attr('cx', function(d) { return d.position.x; })
                    .attr('cy', function(d) { return d.position.y; })
                    .attr('id', function(d) { return d.id; })
                    .style('fill', function(d) {
                        return colorStatus[d.status];
                    });

                NewCenterNodesDom.append('text')
                    .text(function(d) {
                        return d.id;
                    })
                    .attr('text-anchor', 'middle')
                    .style('font-size', '28px');
            }

            graph = extractLinksNodes(data, graph);

            // Note: entries adds the id to the values in the original object
            var nodes = entries(graph.nodes);
            var links = entries(graph.links);
            var status = entries(graph.status);
            var center = entries(graph.center);

            var numberNodes = nodes.length;
            vArcRadius = Math.max((numberNodes*nradius*1.8)/Math.PI, 1000);
            // Calculate number of jobs to be able to fit all triangles into a link.
            var maxJobsCount = 0;
            links.forEach(function(link) {
                var currentCount = Object.keys(link.jobs).length;
                if(currentCount > maxJobsCount) {
                    maxJobsCount = currentCount;
                }
            });
            // 40 is the height of a status job arrow (+2 for borders).
            vArcRadius = Math.max(maxJobsCount * 42 + 5 * nradius, vArcRadius);

            vBox = {
                x: -1 * (vArcRadius + nradius),
                y: -1 * (vArcRadius + nradius),
                w: vArcRadius * 2 + nradius * 2,
                h: vArcRadius + 3 * nradius
            }
            svg.attr('viewBox', vBox.x + ' ' + vBox.y + ' ' + vBox.w + ' ' + vBox.h);

            console.log("center", center);

            svg.selectAll('.center-nodes')
                .data(center, function(d) {
                    return d.id;
                })
                .select('circle')
                .style('fill', function(d) {
                    return colorStatus[d.status];
                });

            var newNodes = svg.selectAll('.node')
                .data(nodes, function(d) {
                    return d.id;
                })
                .enter()
                .append('g')
                .attr("class", "node")
                .on("contextmenu", contextMenu);

            newNodes.append('circle')
                .attr('r', nradius)
                .attr('id', function(d) {
                    return d.id;
                })
                .on('mouseover', showTooltip)
                .on('mousemove', moveTooltip)
                .on('mouseout', hideTooltip)
                .on('click', openErrorView);

            newNodes.append('text')
                .style('font-size', '28px')
                .attr('text-anchor', 'middle')
                .on('mouseover', showTooltip)
                .on('mousemove', moveTooltip)
                .on('mouseout', hideTooltip)
                .each(function(d) {
                    var textNode = d3.select(this);
                    var words = splitText(d.id);
                    var dyStart = Math.ceil(words.length/2) - 1;
                    for(var i = 0; i < words.length; ++i) {
                        textNode
                            .append('tspan')
                            .attr('dy', i === 0? -dyStart + 'em':'1em')
                            .text(words[i]);
                    }
                   
                });

            var allPoints = svg.selectAll('g')
                .filter(function(d) {
                    return !d.position;
                })
                .select('circle');

            var nData = [];
            svg.selectAll('.node').each(function (d) {
                nData.push(d);
            });

            nData.sort(function(a, b) {
                return a.id.localeCompare(b.id);
            });

            var angle = Math.PI / (nData.length + 1);

            var positions = {};
            for(var i = 0; i < nData.length; ++i) {
                var r = vArcRadius - 2 * nradius * (i % 2);
                var x = -r * Math.cos((i + 1) * angle);
                var y = -r * Math.sin((i + 1) * angle);
                positions[nData[i].id] = {x: x, y: y};
            }
            // Append center node positions.
            for(var k in graph.center) {
                positions[k] = graph.center[k].position;
            }

            apply_transition(allPoints, animDuration)
                .attr('cx', function(d) {
                    return positions[d.id].x;
                })
                .attr('cy', function(d) {
                    return positions[d.id].y;
                })
                .style('fill', function(d) {
                    return colorStatus[d.status];
                })

            apply_transition(svg.selectAll('text'), animDuration)
                .attr('x', function(d) {
                    return positions[d.id].x;
                })
                .attr('y', function(d) {
                    return positions[d.id].y;
                });

            apply_transition(svg.selectAll('tspan'), animDuration)
                .attr('x', function(d) {
                    return positions[d.id].x;
                });

            svg.selectAll('line')
                .data(links, function(d) {
                    return d.id;
                })
                .enter()
                .insert('line', '.center-nodes')
                .attr('stroke-width', 6)
                .attr('id', function(d) {
                    return d.id;
                })
                .on('mouseover', showTooltip)
                .on('mousemove', moveTooltip)
                .on('mouseout', hideTooltip)
                .on('contextmenu', contextMenu);


            // Adding connecting links
            var allLinks = svg.selectAll('line');

            apply_transition(allLinks, animDuration)
                .attr('x1', function(d) {
                    if(!positions[d.source]) { return 0; }
                    return positions[d.source].x;
                })
                .attr('y1', function(d) {
                    if(!positions[d.source]) { return 0; }
                    return positions[d.source].y;
                })
                .attr('x2', function(d) {
                    if(!positions[d.target]) { return 0; }
                    return positions[d.target].x;
                })
                .attr('y2', function(d) {
                    if(!positions[d.target]) { return 0; }
                    return positions[d.target].y;
                })
                .attr('stroke', function(d) {
                    var jobsId = Object.keys(d.jobs);
                    for(var i = 0; i < status.length; ++i) {
                        if(d.jobs.hasOwnProperty(status[i].job)) {
                            if(status[i].status == "error") {
                                return 'red';
                            }
                        }
                    }
                    return (d.running === true) ? 'green' : 'lightgrey';
                });

            var linksMid = {};
            allLinks.each(function(d) {
                if(!positions[d.source] || !positions[d.target]) {
                    return;
                }
                var dirX = positions[d.source].x - positions[d.target].x;
                var dirY = positions[d.source].y - positions[d.target].y;
                var dirM = Math.sqrt(dirX*dirX + dirY*dirY);
                if(dirM > 0.01) {
                    dirX /= dirM;
                    dirY /= dirM;
                } else {
                    dirX = 0;
                    dirY = -1;
                }

                // So we are in the center
                var jobsId = Object.keys(d.jobs);

                var pullJobs = [];
                var pushJobs = [];
                for(var i = 0; i < jobsId.length; ++i) {
                    if(d.jobs[jobsId[i]].direction === "pull") {
                        pullJobs.push(d.jobs[jobsId[i]]);
                    } else {
                        pushJobs.push(d.jobs[jobsId[i]]);
                    }
                }

                // TODO: How to merge this two loops in one function... 
                var dir = {x: dirX, y: dirY};
                for(var i = 0; i < pushJobs.length; ++i) {
                    var midX = 0.5 * positions[d.source].x + 0.5 * positions[d.target].x;
                    var midY = 0.5 * positions[d.source].y + 0.5 * positions[d.target].y;
                    linksMid[pushJobs[i].id] = {mid: {x: midX, y: midY}, direction: dir, link: d.id, pull: false};
                }

                dir = {x: -dirX, y: -dirY};
                for(var i = 0; i < pullJobs.length; ++i) {
                    var midX = 0.5 * positions[d.source].x + 0.5 * positions[d.target].x;
                    var midY = 0.5 * positions[d.source].y + 0.5 * positions[d.target].y;
                    linksMid[pullJobs[i].id] = {mid: {x: midX, y: midY}, direction: dir, link: d.id, pull: true};
                }
            });
            console.log("The link mids", linksMid);

            var groupStatus = {};
            status.forEach(function(s) {
                if(linksMid[s.job]) {
                    var linkId = linksMid[s.job].link;
                    if(!groupStatus.hasOwnProperty(linkId)) {
                        // Array to sort properly pullers and pushers along the links
                        groupStatus[linkId] = [];
                    }
                    if(linksMid[s.job].pull) {
                        groupStatus[linkId].push(s.id);
                    } else {
                        groupStatus[linkId].unshift(s.id);
                    }
                }
            });

            var statusPos = {};
            for(var linkId in groupStatus) {
                for(var i = 0; i < groupStatus[linkId].length; ++i) {
                    statusPos[groupStatus[linkId][i]] = i;
                }
            }

            var polySelection = svg.selectAll('polygon')
                .data(status, function(d) {
                    return d.id;
                });

            polySelection.exit().remove();

            polySelection.enter()
                .append('polygon')
                .attr('id', function(d) {
                    return d.id;
                })
                .attr('points', '0,0 -15,40 15,40')
                .on('mouseover', showTooltip)
                .on('mousemove', moveTooltip)
                .on('mouseout', hideTooltip)
                .on('click', openChannelView)
                .on('contextmenu', contextMenu);

            apply_transition(svg.selectAll('polygon'), animDuration)
                .attr('transform', function(d) {
                    if(!linksMid[d.job]) {
                        console.log("Moving outside the visible area as we don't have a position yet", JSON.stringify(d.job));
                        return "translate(0, 250) rotate(180)"
                    } else {
                        var linkId = linksMid[d.job].link;
                        // Trying to center the triangles along the link by shifting by half.
                        var shifted = 0.5 * (groupStatus[linkId].length - 1);
                        var dir = 1;

                        if(!linksMid[d.job].pull) {
                            // Depending in the direction we would like to move up or down.
                            var dir = -1;
                            // Due to rotation we need to move the triangle up a little.
                            shifted -= 1;
                        }

                        var dx = linksMid[d.job].direction.x;
                        var dy = linksMid[d.job].direction.y;
                        var angle = -1 * Math.atan2(dx, dy) * 180 / Math.PI;

                        var x = linksMid[d.job].mid.x + (statusPos[d.id] - shifted) * dx * 40 * dir;
                        var y = linksMid[d.job].mid.y + (statusPos[d.id] - shifted) * dy * 40 * dir;
                        return "translate(" + x + ", " + y + ") rotate(" + angle + ")";
                    }
                })
                .style('stroke', 'black')
                .style('stroke-width', 3)
                .style('fill', function(d) {
                    return colorStatus[d.status];
                });
        },
        on_resize: resize,
        on_reset: function() {
            svg.selectAll('svg > *').remove();
            graph = initGraph();
            firstData = true;
        },
        on_close: function() {
            tooltipDiv.remove();
        }
    };
}
