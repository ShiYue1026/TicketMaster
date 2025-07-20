package com.damai.service.composite.chain;

import com.damai.dto.ProgramOrderCreateDto;

abstract class ProgramOrderCheckHandler {
    protected ProgramOrderCheckHandler nextHandler;

    public void setNextHandler(ProgramOrderCheckHandler nextHandler){
        this.nextHandler = nextHandler;
    }

    public void execute(ProgramOrderCreateDto programOrderCreateDto){
        check(programOrderCreateDto);
        if(nextHandler != null){
            nextHandler.execute(programOrderCreateDto);
        }
    }

    protected abstract void check(ProgramOrderCreateDto programOrderCreateDto);

}
