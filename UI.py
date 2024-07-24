import customtkinter as ctk
class Display:

    def __init__(self):
        self.root = ctk.CTk()
        self.screen_width = self.root.winfo_screenwidth()
        self.screen_height = self.root.winfo_screenheight()
        self.root.geometry(f"{round(self.screen_width*0.78)}x{round(self.screen_height*0.75)}")
        self.root.title("Sistema Integrador")
        ctk.set_default_color_theme("green")
        ctk.set_appearance_mode("dark")
        

        self.frames()
        self.widgets()
        self.current_frame = self.app_frame1
        self.root.iconbitmap("logo_cb.ico")
    def frames(self):

        self.menu_frame = ctk.CTkFrame(self.root, bg_color="#191A19", fg_color="#191A19")
        self.menu_frame.place(relx=0, rely=0, relwidth=0.15, relheight=1)

        #Frame 1
        self.app_frame1 = ctk.CTkFrame(self.root, fg_color="#141414")
        self.app_frame1.place(relx=0.15, rely=0, relwidth=0.85, relheight=1)

        self.app_subframe1 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe1.place(relx=0, rely=0, relwidth=0.5, relheight=0.5)

        self.app_subframe2 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe2.place(relx=0.5, rely=0, relwidth=0.5, relheight=0.5)

        self.app_subframe3 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe3.place(relx=0, rely=0.5, relwidth=0.5, relheight=0.5)

        self.app_subframe4 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe4.place(relx=0.5, rely=0.5, relwidth=0.5, relheight=0.5)

        #Frame 2
        self.app_frame2 = ctk.CTkFrame(self.root, bg_color="#141414", fg_color="#141414")

        self.app2_subframe = ctk.CTkFrame(self.app_frame2, fg_color="#141414")
        self.app2_subframe.place(relx=0, rely=0, relwidth=0.5, relheight=0.5)

        self.app2_subframe2 = ctk.CTkFrame(self.app_frame2, fg_color="#191A19")
        self.app2_subframe2.place(relx=0.5, rely=0.02, relwidth=0.5, relheight=0.96)

    def widgets(self):
        #Widgets Modulo 1
        self.modulo1_label = ctk.CTkLabel(self.app_subframe1,text="Painel Principal", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo1_label.place(relx=0.02, rely=0.04)

        self.modulo1_textbox = ctk.CTkTextbox(self.app_subframe1, width=0, height=0, font=("Montserrat", 14),fg_color="#282A27")
        self.modulo1_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Modulo 2
        self.modulo2_label = ctk.CTkLabel(self.app_subframe2,text="Modulo 1", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo2_label.place(relx=0.02, rely=0.04)

        self.modulo2_textbox = ctk.CTkTextbox(self.app_subframe2, width=0, height=0, font=("Montserrat", 14),  fg_color="#282A27")
        self.modulo2_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Modulo 3
        self.modulo3_label = ctk.CTkLabel(self.app_subframe3,text="Modulo 2", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo3_label.place(relx=0.02, rely=0.04)

        self.modulo3_textbox = ctk.CTkTextbox(self.app_subframe3, width=0, height=0, font=("Montserrat", 14),fg_color="#282A27")
        self.modulo3_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Modulo 4
        self.modulo4_label = ctk.CTkLabel(self.app_subframe4,text="Modulo 3", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo4_label.place(relx=0.02, rely=0.04)

        self.modulo4_textbox = ctk.CTkTextbox(self.app_subframe4, width=0, height=0, font=("Montserrat", 14),fg_color="#282A27")
        self.modulo4_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Frame 2
        self.error_label = ctk.CTkLabel(self.app2_subframe,text="Mensagens de Erro", font=("Montserrat", 18, "bold"))
        self.error_label.place(relx=0.02, rely=0.04)

        self.error_textbox = ctk.CTkTextbox(self.app2_subframe, width=0, height=0, font=("Montserrat", 14), fg_color="#282A27")
        self.error_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        self.info1_label = ctk.CTkLabel(self.app2_subframe2, font=("Montserrat", 14, "bold"), text="Produtos atualizados: ")
        self.info1_label.place(relx=0.03, rely=0.02, relwidth=0.3, relheight=0.05)
        self.info1_count = ctk.CTkLabel(self.app2_subframe2, font=("Montserrat", 14, "bold"), text="0")
        self.info1_count.place(relx=0.32, rely=0.02, relwidth=0.15, relheight=0.05)

        #Menu Buttons
        std_x = 0.05
        std_width = 0.9
        std_font1 = ("Montserrat", 12, "bold")
        std_font2 = ("Montserrat", 14, "bold")
        self.start_btn = ctk.CTkButton(self.menu_frame, text="Iniciar Sincronização",font=std_font1 ,command=self.start)
        self.start_btn.place(relx=std_x, relwidth=std_width, rely=0.02, relheight=0.05)

        self.pause_btn = ctk.CTkButton(self.menu_frame, text="Pausar Sincronização",font=std_font1, command=self.pause_thread, state="disabled")
        self.pause_btn.place(relx=std_x, relwidth=std_width, rely=0.09, relheight=0.05)

        self.continue_btn = ctk.CTkButton(self.menu_frame, text="Continuar Sincronização", font=std_font1,command=self.continuar_thread, state="disabled")
        self.continue_btn.place(relx=std_x, relwidth=std_width, rely=0.16, relheight=0.05)

        self.stop_btn = ctk.CTkButton(self.menu_frame, text="Parar Sincronização",font=std_font1, command=self.parar_thread, state="disabled")
        self.stop_btn.place(relx=std_x, relwidth=std_width, rely=0.23, relheight=0.05)

        self.f1_btn = ctk.CTkButton(self.menu_frame, text="Painel Principal",font=std_font2, command=lambda: self.switch_frame(self.app_frame1))
        self.f1_btn.place(relx=std_x, relwidth=std_width, rely= 0.5, relheight=0.1)

        self.f2_btn = ctk.CTkButton(self.menu_frame, text="Painel de Informações",font=std_font2, command=lambda: self.switch_frame(self.app_frame2))
        self.f2_btn.place(relx=std_x, relwidth=std_width, rely= 0.62, relheight=0.1)

        self.start_btn.bind("<Configure>", lambda event: self.resize_text(event, self.start_btn))
        self.pause_btn.bind("<Configure>", lambda event: self.resize_text(event, self.pause_btn))
        self.continue_btn.bind("<Configure>", lambda event: self.resize_text(event, self.continue_btn))
        self.stop_btn.bind("<Configure>", lambda event: self.resize_text(event, self.stop_btn))
        self.f1_btn.bind("<Configure>", lambda event: self.resize_text(event, self.f1_btn))
        self.f2_btn.bind("<Configure>", lambda event: self.resize_text(event, self.f2_btn))

        # self.f3_btn = ctk.CTkButton(self.menu_frame, text="Get TxtBox", command= self.get_txtbox)
        # self.f3_btn.place(relx=0.2, relwidth=0.6, rely= 0.8, relheight=0.1)

class UIFunctions(Display):
    def __init__(self):
        super().__init__() 

    def switch_frame(self,target_frame):
        self.current_frame.place_forget()
        target_frame.place(relx=0.15, rely=0, relwidth=0.85, relheight=1)
        self.current_frame = target_frame

    def get_txtbox(self):
        texto = self.modulo1_textbox.get("1.0", ctk.END)
        self.modulo2_textbox.insert(ctk.END, texto)
    
    def resize_text(self, event, button):
    # Ajusta o comprimento da quebra de linha com base na largura do botão
        wrap_length = event.width  # Ajuste conforme necessário
        button._text_label.configure(wraplength=wrap_length)
