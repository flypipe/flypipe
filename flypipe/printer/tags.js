function action_tag(tag, action){
    console.log(tag, action);
    if (action == 'highlight') {
        for (let i = 0; i < nodes.length; i++) {
            node = nodes[i];
            if (node.definition.tags.includes(tag)){
                highlight_node(node.name);
            }
        }
    }
    else if (action == 'suppress') {
        still_selected_tags = $('input[name="tags"]').val().split(",");

        for (let i = 0; i < nodes.length; i++) {
            node = nodes[i];

            if (node.definition.tags.includes(tag) & highlighted_nodes.has(node.name)){

                still_contain_tag = (still_selected_tags.length > 0) ? false : true;

                for (let j = 0; j < still_selected_tags.length; j++) {
                    still_selected_tag = still_selected_tags[j];
                    if (node.definition.tags.includes(still_selected_tag)){
                        still_contain_tag = true;
                        break;
                    }
                }

                if (!still_contain_tag){
                    suppress_node(node.name);
                }


            }
        }
    }

}

ff = $('input[name="tags"]').amsifySuggestags({
    suggestions: tags,
    whiteList: true,
    defaultLabel: 'Filter tag',
    callback: action_tag
});
