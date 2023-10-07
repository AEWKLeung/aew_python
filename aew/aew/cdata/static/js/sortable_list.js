function checkBoxCheck(e) {
    if ($('input[type=checkbox]:checked').length) {
        // console.log("at least one checked");
        return true;
    } else {
        // console.error("no checkbox checked");
        return false;
    }
}

function init_result_table() {
    // Check files have been selected for upload
    if (document.getElementById("lab_data_input").value == "") {
        // submit empty form to flash flask error if no files selected
        document.getElementById("main_form").submit();
    // Check regulatory criteria have been selected 
    } else if (checkBoxCheck(document.getElementById("main_form")) == false) {
        alert("You must select at least one regulation to include before continuing")
        return;
    }
    // Add loading symbol
    document.getElementById("result_table_init").classList.add("is-loading")

    // Send files to flask route and get back sample names for sort list
    var form_data = new FormData(document.getElementById('main_form'));
    $.ajax({
        type: 'POST',
        url: '/table_generator_init',
        data: form_data,
        contentType: false,
        cache: false,
        processData: false,
        // Present user with sortable list
        success: function(data) {
            // Add list items
            $.each(data, function(index, samp_id) {
                var list_item = '<li draggable="false" class="" style="">' + samp_id + '</li>'
                $('#sample_order').append(list_item);
                // Make modal visible
                var element = document.getElementById("sample_order_modal");
                element.classList.add("is-active");
                // Remove loading symbol
                document.getElementById("result_table_init").classList.remove("is-loading")

            });

        },
    });


}