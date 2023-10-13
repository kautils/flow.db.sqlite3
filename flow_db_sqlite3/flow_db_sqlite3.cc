#include "flow_db_sqlite3.h"
#include "kautil/sqlite3/sqlite3.h"
#include <libgen.h>
#include <sys/stat.h>
#include <string>


struct io_data{ const void * begin=0;const void * end=0; uint64_t block_size=0;  uint64_t nitems=0; };
struct filter_database_sqlite3_handler{
    kautil::database::Sqlite3Stmt * create=0;
    kautil::database::Sqlite3Stmt * insert=0;
    kautil::database::Sqlite3 * sql=0;
    kautil::database::sqlite3::Options * op=0;
    std::string path;
    io_data i;
    io_data o;
    uint64_t * index=0;
    bool is_overwrite=false;
    bool is_without_rowid=false;
    bool is_value_uniformed=false;
    bool is_key_uniformed=false;
};



constexpr static const char * kCreateSt             = "create table if not exists m([rowid] interger primary key,[k] blob,[v] blob,unique([k])) ";
constexpr static const char * kCreateStWithoutRowid = "create table if not exists m([k] blob primary key,[v] blob) without rowid ";
static const char * kInsertSt = "insert or ignore into m(k,v) values(?,?)";
static const char * kInsertStOw = "insert or replace into m(k,v) values(?,?)";

int mkdir_recurse(const char * path_to_dir);
filter_database_sqlite3_handler* get_instance(void * whdl){
    return reinterpret_cast<filter_database_sqlite3_handler*>(whdl);
}

void* filter_database_sqlite_initialize(){
    auto res = new filter_database_sqlite3_handler;
    res->op = kautil::database::sqlite3::sqlite_options(); 
    return res;
}

void filter_database_sqlite_free(void* whdl){
    auto m=get_instance(whdl);
    delete m->op;
    if(m->create)m->create->release();
    if(m->insert)m->insert->release();
    delete m->sql;
    delete m;
}


uint64_t filter_database_sqlite_uri_hasher(const char * prfx,const char * filter_id,const char * state_id){
    return std::hash<std::string>{}(std::string(prfx)+filter_id+state_id);
}

int filter_database_sqlite_set_uri(void * whdl,const char * prfx,const char * filter_id,const char * state_id){
    auto m=get_instance(whdl);
    if(prfx) (m->path = prfx);
    else m->path = "./filter_db_sqlite3/";
    
    if(0== bool(filter_id) + bool(state_id)){
        m->path = "/not_specified";
    }else{
        if(filter_id)m->path.append("/").append(filter_id);
        if(state_id)m->path.append("/").append(state_id);
    }
    m->path.append(".sqlite");
    return 0;
}

int filter_database_sqlite_setup(void * whdl){
    
    auto m=get_instance(whdl);
    if(!m->sql) {
        {
            auto buf = std::string{m->path.data()};
            auto dir = dirname(buf.data());
            if(mkdir_recurse(dir)){
                printf("fail to create directory \"%s\"\n",m->path.data());
                return 1;
            }
        }
        
        m->sql = new kautil::database::Sqlite3{m->path.data(),m->op->sqlite_open_create()|m->op->sqlite_open_readwrite()|m->op->sqlite_open_nomutex()};
        m->create = m->sql->compile(m->is_without_rowid ? kCreateStWithoutRowid : kCreateSt);
        if(m->create){
            auto res_crt = m->create->step(true);
            res_crt |= ((m->create->step(true) == m->op->sqlite_ok()));
            if(res_crt){
                return !bool(m->insert = m->sql->compile(m->is_overwrite ? kInsertStOw : kInsertSt));
            }
        }
    }
    m->sql->error_msg();
    if(m->create) m->create->release();
    if(m->insert) m->insert->release();
    delete m->op;
    delete m->sql;
    m->op=nullptr;
    m->sql=nullptr;
    
    return 1;
}


int filter_database_sqlite_set_index(void * whdl,uint64_t * begin){
    auto m=get_instance(whdl);
    m->index = begin;
    return !bool(m->index); 
}

int filter_database_sqlite_set_output(void * whdl,const void * begin,uint64_t block_size,uint64_t nitems){
    auto m=get_instance(whdl);
    m->o.begin = begin;
    m->o.end = (void*) (uintptr_t(begin)+uintptr_t(nitems*block_size));
    m->o.nitems=nitems;
    m->o.block_size=block_size;
    return m->o.begin < m->o.end; 
}

int filter_database_sqlite_set_input(void * whdl,const void * begin,uint64_t block_size,uint64_t nitems){
    auto m=get_instance(whdl);
    m->i.begin = begin;
    m->i.end = (void*) (uintptr_t(begin) + uintptr_t(block_size*nitems));
    m->i.nitems=nitems;
    m->i.block_size=block_size;
    return m->i.begin < m->i.end; 
}


int filter_database_sqlite_sw_overwrite(void * whdl,bool sw){
    auto m=get_instance(whdl);
    m->is_overwrite=sw;
    return 0;
}

int filter_database_sqlite_sw_without_rowid(void * whdl,bool sw){
    auto m=get_instance(whdl);
    m->is_without_rowid=sw;
    return 0;
}

int filter_database_sqlite_sw_uniformed(void * whdl,bool sw){
    auto m=get_instance(whdl);
    m->is_value_uniformed=sw;
    return 0;
}

int filter_sw_key_is_uniformed(void * whdl,bool sw){
    auto m=get_instance(whdl);
    m->is_key_uniformed=sw;
    return 0;
}



static bool update_sqlite(filter_database_sqlite3_handler * m
        ,const char * begin_i,uint64_t block_i
        ,const char * begin_o,uint64_t block_o
        ){
    
    if(!m->is_key_uniformed){
        block_i=reinterpret_cast<const uint64_t*>(begin_i)[1]; 
        begin_i=(const char *)reinterpret_cast<const uint64_t*>(begin_i)[0]; 
    }
    
    if(!m->is_value_uniformed){
        block_o=reinterpret_cast<const uint64_t*>(begin_o)[1]; 
        begin_o=(const char *)reinterpret_cast<const uint64_t*>(begin_o)[0]; 
    }
    
    auto res_stmt = !m->insert->set_blob(1,begin_i,block_i);
    res_stmt |= !m->insert->set_blob(2,begin_o,block_o);

    auto res_step = m->insert->step(true);
    res_step |= res_step == m->op->sqlite_ok();
    
    return res_stmt+!res_step;
}



int filter_database_sqlite_save(void * whdl){
    auto m=get_instance(whdl);
    if(auto begin_o = reinterpret_cast<const char*>(m->o.begin)){
        auto end_o = reinterpret_cast<const char*>(m->o.end);
        auto block_o = m->o.block_size;
        auto fail = false;
        
        if(auto begin_i = reinterpret_cast<const char*>(m->i.begin)){
            auto end_i = reinterpret_cast<const char*>(m->i.end);
            auto block_i = m->i.block_size;
            
            if(0== !(begin_i < end_i) + !(begin_o < end_o)){
                if(m->index){ // limit : the result shrinked
                    auto org_i= begin_i;
                    for(auto i = 0; i < m->o.nitems; ++i,begin_i=org_i+(block_i*m->index[i]),begin_o+=block_o ){
                        if((fail=update_sqlite(m,begin_i,block_i,begin_o,block_o)))break;
                    }
                }else{ // full
                    for(;begin_i != end_i; begin_i+=block_i,begin_o+=block_o){
                        if((fail=update_sqlite(m,begin_i,block_i,begin_o,block_o)))break;
                    }
                    
                }
            }
            if(fail) m->sql->roll_back();
            m->sql->end_transaction();
            return fail;
        }
    }else m->sql->error_msg();
    return 1;
}


struct lookup_protocol_table{};
struct lookup_protocol_elem{
    const char * key=nullptr;
    void * value=nullptr;
};

struct lookup_protocol_table_database_sqlite{
    lookup_protocol_elem initialize{.key="initialize",.value=(void*)filter_database_sqlite_initialize};
    lookup_protocol_elem free{.key="free",.value=(void*)filter_database_sqlite_free};
    lookup_protocol_elem uri_hasher{.key="uri_hasher",.value=(void*) filter_database_sqlite_uri_hasher};
    lookup_protocol_elem set_uri{.key="set_uri",.value=(void*)filter_database_sqlite_set_uri};
    lookup_protocol_elem setup{.key="setup",.value=(void*)filter_database_sqlite_setup};
    lookup_protocol_elem set_output{.key="set_output",.value=(void*)filter_database_sqlite_set_output};
    lookup_protocol_elem set_index{.key="set_index",.value=(void*)filter_database_sqlite_set_index};
    lookup_protocol_elem set_input{.key="set_input",.value=(void*)filter_database_sqlite_set_input};
    lookup_protocol_elem sw_overwrite{.key="sw_overwrite",.value=(void*)filter_database_sqlite_sw_overwrite};
    lookup_protocol_elem sw_rowid{.key="sw_without_rowid",.value=(void*)filter_database_sqlite_sw_without_rowid};
    lookup_protocol_elem sw_uniformed{.key="sw_uniformed",.value=(void*)filter_database_sqlite_sw_uniformed};
    lookup_protocol_elem sw_key_is_uniformed{.key="sw_key_is_uniformed",.value=(void*)filter_sw_key_is_uniformed};
    lookup_protocol_elem save{.key="save",.value=(void*)filter_database_sqlite_save};
//    lookup_protocol_elem member{.key="member",.value=(void*)filter_database_sqlite_initialize()};
    lookup_protocol_elem member{.key="member",.value=nullptr};
    lookup_protocol_elem sentinel{.key=nullptr,.value=nullptr};
} __attribute__((aligned(sizeof(uintptr_t))));


extern "C" uint64_t  size_of_pointer(){ return sizeof(uintptr_t);}
extern "C" lookup_protocol_table * lookup_table_initialize(){ 
    auto res= new lookup_protocol_table_database_sqlite{}; 
    return reinterpret_cast<lookup_protocol_table*>(res);
}
extern "C" void lookup_table_free(lookup_protocol_table * f){
    auto entity = reinterpret_cast<lookup_protocol_table_database_sqlite*>(f);
    delete reinterpret_cast<filter_database_sqlite3_handler*>(entity->member.value);
    delete entity; 
}




int mkdir_recurse(char * p){
    
    auto c = p;
    struct stat st;

    if(0==stat(p,&st)){
        return !S_ISDIR(st.st_mode);
    } 
    auto b = true;
    for(;b;++c){
        if(*c == '\\' || *c=='/'){
            ++c;
            auto evacu = *c;
            *c = 0;
            if(stat(p,&st)){
                if(mkdir(p)) b = false;
            }
            *c = evacu;
        }
        if(0==!b+!*c) continue;
        if(stat(p,&st)){
            if(mkdir(p)) b = false;
        }
        break;
    }
    return !b;
}




int tmain_kautil_flow_db_sqlite3_shared(){
    
    exit(0);
    return 0;
}
