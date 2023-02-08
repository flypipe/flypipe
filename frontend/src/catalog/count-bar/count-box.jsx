import React from 'react';

const CountBox = ({label, count}) => {
    return <div className="d-flex flex-column mx-4 my-2 px-3 py-2 shadow-sm rounded bg-info bg-opacity-25 align-items-center" style={{minWidth: "100px", minHeight: "100px"}}>
        <h2>{count}</h2>
        <p className='fw-bold'>{label}</p>
    </div>
};

export default CountBox;