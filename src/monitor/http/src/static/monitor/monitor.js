require.config({
    packages: [
        {
            name: 'echarts',
            location: './static/echarts',
            main: 'echarts'
        }
    ]
});
require(
[
    'echarts',
    'echarts/chart/line'
],
function (ec) {
    var config = {teraIO:"tera-io",teraOP:'tera-op'};
    var notice = new LoadingNotice();
    var nodeStat = new NodeStat(ec,config,notice);
    $('#date-start-input').datetimepicker();
    $('#date-end-input').datetimepicker();
    $("#node-select").select2({
        placeholder: "Select a Node",
        allowClear: true
    });
    var handleNodeSelect = function(nodeName){
        var start_time = $('#date-start-input').val();
        var end_time = $('#date-end-input').val();

        var zkNode = $('#node-select').select2('val');
        nodeStat.render(zkNode,nodeName,start_time,end_time,null);
    }
    var handleZkChange = function(newZkNode){
        var reqUrl = "/monitor/getNodeList?zknode="+newZkNode;
        notice.info("loading nodes ...");
        $.getJSON(reqUrl,function(data){
            notice.hide();
            var html = "";
            for(var i in data.data){
                html+= '<li style="cursor:pointer" class="list-group-item">'+data.data[i]+'</li>';
            }
            $('#node-list').html(html);
            $('#node-list li').click(function(e){
                $('#node-list li').removeClass("active");
                $(e.target).addClass("active");
                handleNodeSelect($(e.target).text());
            });
        });
     }

   $('#node-select').on("select2-selecting",function(e){
        handleZkChange(e.val);
    });
    var initZk = $('#node-select').select2('val');
    if(initZk){
        handleZkChange(initZk);
    }
    $('#cs-query').click(function(){
    	var csZk = $('#cs-zk').val();
    	if(!csZk){
    		alert("请输入zk");
    		return;
    	}
    	var zkPath = $('#zk-path').val();
    	if(!zkPath){
    		alert("请输入zk路径");
    		return;
    	}
    	var teraNode = $('#tera-node').val();
    	if(!teraNode){
    		alert("请输入节点");
    		return;
    	}
    	var start_time = $('#date-start-input').val();
        var end_time = $('#date-end-input').val();
        nodeStat.render(csZk,teraNode,start_time,end_time,zkPath);
    });
}
);
