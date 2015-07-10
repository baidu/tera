

/**
 * 主要用对后端请求数据时，进行相关的提示
 * */
var LoadingNotice = function(){
    // do some init
    $('#notice-span').remove();
    var noticeHtml = '<span id="notice-span" style="display: none;' +
                                                   'background-color:#FDF390;' +
                                                   'padding:5px 30px ;' +
                                                   'border-radius:1px;' +
                                                   'font-weight:900;' +
                                                   'line-height:1em;' +
                                                   'font-size:1em;' +
                                                   'color:#302D2D;' +
                                                   'position:fixed;' +
                                                   'left:45%;' +
                                                   'top:0;' +
                                                   'z-index:9999999' +
                                                   '"></span>';
    $('body').append(noticeHtml);
    this.$noticeSpan = $('#notice-span');
};
LoadingNotice.prototype.info = function(msg){
    this.$noticeSpan.html(msg);
    this.$noticeSpan.css("background-color","#FDF390");
    this.$noticeSpan.show();
};
LoadingNotice.prototype.alert = function(msg,fadeout){
    this.$noticeSpan.html(msg);
    this.$noticeSpan.css("background-color","#d59595");
    this.$noticeSpan.fadeOut(fadeout);
};
LoadingNotice.prototype.hide = function(){
    this.$noticeSpan.hide();
};
var NodeStat = function(ec, config, notice) {
	this.config = config;
	this.teraIO = ec.init(document.getElementById(config.teraIO));
	this.teraOP = ec.init(document.getElementById(config.teraOP));
    this.notice = notice;
};
NodeStat.prototype.isEmpty = function(obj){
    for (var name in obj){
        return true;
    }
    return false;
}
NodeStat.prototype.render = function(zk,nodeName,startTime,endTime,zkPath){
	var self = this;
	this.getLine(zk,nodeName,startTime,endTime,zkPath,function(statline){
        if(statline.data==null || self.isEmpty(statline.data) ){
        	self.renderIOLine(statline);
	    	self.renderOP(statline);
        }else{
           self.notice.alert("no data to show");
        }
		});
}
/**
 * 从远程获取某个节点的状态
 */
NodeStat.prototype.getLine = function(zk,nodename,startTime,endTime,zkPath,callback) {
	var reqUrl = "/monitor/statLine?node=" + nodename+"&start="+startTime+"&end="+endTime+"&zk="+zk;
	if(zkPath){
		reqUrl+="&zkPath="+zkPath;
	}
    this.notice.info("loading node status ...");
    var self = this;
    $.getJSON(reqUrl, function(data) {
        self.notice.hide();
		callback(data.data);
	});
}
NodeStat.prototype.renderOP = function(statline){
	var option = {
			title : {
				text : 'tera rows',
				subtext : '',
				x : 'center'
			},
			tooltip : {
				trigger : 'axis',
				formatter : function(params) {
					var label = params[0].name + '<br/>';
					for(var index in params){
						label += params[index].seriesName + ' : ' + params[index].value + '<br/>';
					}
					 return label;
				}
			},
			legend : {
				data : [ 'low_read_cell','scan_rows','scan_size','read_rows','write_rows','write_size'],
				x:"30",
				y:"30"
				
			},
			dataZoom : {
				show : true,
				realtime : true,
				start : 0,
				end : 100
			},
			toolbox : {
				show : false
			},
			xAxis : [ {
				type : 'category',
				boundaryGap : false,
				axisLine : {
					onZero : false
				},
				data : statline.time_stamp
			} ],
			yAxis : [ {
				type : 'value'
			} ,{
				type : 'value'
			}],
			series : [ {
				name : 'low_read_cell',
				type : 'line',
				itemStyle : {
					normal : {
						areaStyle : {
							type : 'default'
						}
					}
				},
				yAxisIndex: 1,
				data : statline.low_read_cell
			}, {
				name : 'scan_rows',
				type : 'line',
				itemStyle : {
					normal : {
						areaStyle : {
							type : 'default'
						}
					}
				},
				data : statline.scan_rows
			} , {
				name : 'scan_size',
				type : 'line',
				itemStyle : {
					normal : {
						areaStyle : {
							type : 'default'
						}
					}
				},
				yAxisIndex: 1,
				data : statline.scan_size
			} ,
			{
				name : 'read_rows',
				type : 'line',
				itemStyle : {
					normal : {
						areaStyle : {
							type : 'default'
						}
					}
				},
				data : statline.read_rows
			} ,
			{
				name : 'write_rows',
				type : 'line',
				itemStyle : {
					normal : {
						areaStyle : {
							type : 'default'
						}
					}
				},
				data : statline.write_rows
			} ,
			{
				name : 'write_size',
				type : 'line',
				itemStyle : {
					normal : {
						areaStyle : {
							type : 'default'
						}
					}
				},
				 yAxisIndex: 1,
				data : statline.write_size
			} ]
		};
	this.teraOP.setOption(option,"dark");
}
/**
 * 渲染io状态
 */
NodeStat.prototype.renderIOLine = function(statline) {
	var option = {
		title : {
			text : 'tera io',
			subtext : '',
			x : 'center'
		},
		tooltip : {
			trigger : 'axis',
			formatter : function(params) {
				var label = params[0].name + '<br/>';
				for(var index in params){
					if( params[index].seriesName == "mem_used"){
						label += params[index].seriesName + ' : ' + params[index].value + ' G<br/>';
					}
					else{
						label += params[index].seriesName + ' : ' + params[index].value + ' M<br/>';
					}
					
				}
				 return label;
			}
		},
		legend : {
			data : [ 'net_tx','net_rx','mem_used','dfs_io_r','dfs_io_w','local_io_r','local_io_w' ],
			x:"30",
			y:"30"
		},
		dataZoom : {
			show : true,
			realtime : true,
			start : 0,
			end : 100
		},
		toolbox : {
			show : false
		},
		xAxis : [ {
			type : 'category',
			boundaryGap : false,
			axisLine : {
				onZero : false
			},
			data : statline.time_stamp
		} ],
		yAxis : [ {
			type : 'value'
		} ],
		series : [ {
			name : 'net_tx',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.net_tx
		}, {
			name : 'net_rx',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.net_rx
		} , {
			name : 'mem_used',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.mem_used
		} ,
		{
			name : 'dfs_io_r',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.dfs_io_r
		} ,
		{
			name : 'dfs_io_w',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.dfs_io_w
		} ,
		{
			name : 'local_io_r',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.local_io_r
		} ,
		{
			name : 'local_io_w',
			type : 'line',
			itemStyle : {
				normal : {
					areaStyle : {
						type : 'default'
					}
				}
			},
			data : statline.local_io_w
		}]
	};
	this.teraIO.setOption(option,"dark");
	
}
