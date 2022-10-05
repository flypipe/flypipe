function action_tag(tag, action){
    f = (action == 'highlight') ?  highlight_node : suppress_node;
    for (let i = 0; i < nodes.length; i++) {
        node = nodes[i];
        if (node.definition.tags.includes(tag)){
            f(node.name);
        }
    }
}

ff = $('input[name="tags"]').amsifySuggestags({
    suggestions: tags,
    whiteList: true,
    defaultLabel: 'Filter tag',
    callback: action_tag
});
