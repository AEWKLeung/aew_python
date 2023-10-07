function checkBoxCheck(e){if($('input[type=checkbox]:checked').length){return true;}else{return false;}}
function wait(ms){var start=new Date().getTime();var end=start;while(end<start+ms){end=new Date().getTime();}}
function init_result_table(){if(document.getElementById("lab_data_input").value==""){document.getElementById("main_form").submit();}else if(checkBoxCheck(document.getElementById("main_form"))==false){alert("You must select at least one regulation to include before continuing")
return;}
document.getElementById("result_table_init").classList.add("is-loading")
wait(2000);var form_data=new FormData(document.getElementById('main_form'));$.ajax({type:'POST',url:'/table_generator_init',data:form_data,contentType:false,cache:false,processData:false,success:function(data){$.each(data,function(index,samp_id){var list_item='<li draggable="false" class="" style="">'+samp_id+'</li>'
$('#sample_order').append(list_item);var element=document.getElementById("sample_order_modal");element.classList.add("is-active");document.getElementById("result_table_init").classList.remove("is-loading")});},});}