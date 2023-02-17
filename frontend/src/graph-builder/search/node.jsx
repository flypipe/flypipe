import React, {useCallback, useMemo} from 'react';
import classNames from 'classnames';


const Node = ({nodeKey, name, importCmd, description, isInGraphBuilder=false, selected, handleClickNode, handleClickGraphBuilder}) => {
    const graphBuilderButton = useMemo(() => {
        return <button 
            className={"btn btn-sm btn-light"}
            data-elem-name="graph-builder-button"
            onClick={() => {handleClickGraphBuilder(nodeKey)}}
            data-toggle="tooltip" 
            data-placement="top" 
            title={"Add node to the Graph Builder"}
        >
            Add
        </button>
    }, [isInGraphBuilder, nodeKey]);

    return <a className={classNames(
        "list-group-item", 
        "list-group-item-action",
        {
            "active": selected
        }
    )} onClick={(e) => {
        if (!(e.target.getAttribute('data-elem-name') === "graph-builder-button")) {
            handleClickNode(nodeKey);
        }
    }}
    >
        <div className="d-flex justify-content-between">
            <label className="form-check-label" htmlFor={`nodeCheckbox-${name}`}><span className="fw-bold">{name}</span></label>
            {graphBuilderButton}
        </div>
        <p>{description}</p>
    </a>
};

export default Node;